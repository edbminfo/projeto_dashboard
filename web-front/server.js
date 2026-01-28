const express = require('express');
const bodyParser = require('body-parser');
const session = require('express-session');
const { Pool } = require('pg');
const axios = require('axios');

const app = express();
const PORT = 3000;

// ==================================================================
// 1. CONFIGURAÇÕES & AMBIENTE
// ==================================================================

const N8N_WEBHOOK_URL = "https://webhooksweb.n8n.bmhelp.click/webhook/api-whats";
const N8N_TOKEN_AUTH = "42d971fd-452b-44d9-9831-7c1ac35f3ef2"; 
const INSTANCIA_WHATS = "31262050";
const SENHA_ADMIN_WEBHOOK = process.env.SENHA_ADMIN_SISTEMA || "SenhaParaCriarNovosClientes";

const pool = new Pool({
    user: process.env.DB_USER || 'admin_dash',
    host: process.env.DB_HOST || 'localhost', 
    database: process.env.DB_NAME || 'dashboard_multitenant',
    password: process.env.DB_PASS || 'SuaSenhaSeguraDoBanco123',
    port: process.env.DB_PORT || 5411, 
});

// ==================================================================
// 2. MIDDLEWARES
// ==================================================================

app.use(bodyParser.urlencoded({ extended: true })); 
app.use(bodyParser.json()); 
app.use(express.static('public')); 
app.set('view engine', 'ejs'); 

// Sessão Persistente (30 dias)
app.use(session({
    secret: 'segredo-super-seguro-bmhelp',
    resave: false,
    saveUninitialized: false,
    cookie: { 
        secure: false, 
        httpOnly: true,
        maxAge: 1000 * 60 * 60 * 24 * 30 // 30 dias
    }
}));

// ==================================================================
// 3. FUNÇÕES AUXILIARES
// ==================================================================

function gerarToken() {
    return Math.floor(100000 + Math.random() * 900000).toString();
}

function authGuard(req, res, next) {
    if (req.session.usuario) return next();
    res.redirect('/login');
}

// ==================================================================
// 4. ROTAS DE AUTENTICAÇÃO
// ==================================================================

app.get('/login', (req, res) => {
    res.render('login', { erro: null });
});

app.post('/auth/solicitar', async (req, res) => {
    const { telefone } = req.body;
    const foneLimpo = telefone.replace(/\D/g, '');

    if (foneLimpo.length < 8) {
        return res.render('login', { erro: "Número inválido." });
    }

    try {
        let querySQL = "";
        let params = [];

        // Busca Inteligente (DDD + Sufixo)
        if (foneLimpo.length >= 10) {
            const ddd = foneLimpo.substring(0, 2);
            const sufixo = foneLimpo.slice(-8);
            querySQL = "SELECT * FROM usuarios WHERE telefone LIKE $1";
            params = [`%${ddd}%${sufixo}`]; 
        } else {
            querySQL = "SELECT * FROM usuarios WHERE telefone LIKE $1";
            params = [`%${foneLimpo}`];
        }
        
        const userCheck = await pool.query(querySQL, params);
        
        if (userCheck.rows.length === 0) {
            return res.render('login', { erro: "Usuário não encontrado." });
        }

        const usuarioEncontrado = userCheck.rows[0];
        const telefoneOficial = usuarioEncontrado.telefone; 

        const tokenAcesso = gerarToken();
        const expira = new Date(Date.now() + 5 * 60000);
        
        await pool.query("INSERT INTO sessoes_login (telefone, token_acesso, expira_em) VALUES ($1, $2, $3)", 
            [telefoneOficial, tokenAcesso, expira]);

        const payloadN8n = {
            "token": N8N_TOKEN_AUTH,
            "tipo": "text",
            "instancia": INSTANCIA_WHATS,
            "telefone": telefoneOficial,
            "cont": `Seu código de acesso ao Dashboard BM: *${tokenAcesso}*`
        };

        await axios.post(N8N_WEBHOOK_URL, payloadN8n);

        req.session.temp_telefone = telefoneOficial;
        res.redirect('/verificar');

    } catch (erro) {
        console.error("Erro no login:", erro);
        res.render('login', { erro: "Erro ao processar login." });
    }
});

app.get('/verificar', (req, res) => {
    if (!req.session.temp_telefone) return res.redirect('/login');
    res.render('verificar', { telefone: req.session.temp_telefone, erro: null });
});

app.post('/auth/validar', async (req, res) => {
    const { codigo } = req.body;
    const telefone = req.session.temp_telefone;

    try {
        const resultado = await pool.query(
            "SELECT * FROM sessoes_login WHERE telefone = $1 AND token_acesso = $2 AND usado = FALSE AND expira_em > NOW()",
            [telefone, codigo]
        );

        if (resultado.rows.length > 0) {
            await pool.query("UPDATE sessoes_login SET usado = TRUE WHERE id = $1", [resultado.rows[0].id]);
            const user = await pool.query("SELECT * FROM usuarios WHERE telefone = $1", [telefone]);
            req.session.usuario = user.rows[0];
            res.redirect('/');
        } else {
            res.render('verificar', { telefone, erro: "Código inválido." });
        }
    } catch (erro) {
        console.error(erro);
        res.redirect('/login');
    }
});

app.get('/logout', (req, res) => {
    req.session.destroy();
    res.redirect('/login');
});

// ==================================================================
// 5. ROTA HOME
// ==================================================================

app.get('/', authGuard, async (req, res) => {
    try {
        const query = `
            SELECT l.id 
            FROM lojas_sincronizadas l
            JOIN usuarios_lojas ul ON l.id = ul.loja_id
            WHERE ul.usuario_id = $1
            ORDER BY l.id ASC
            LIMIT 1
        `;
        const result = await pool.query(query, [req.session.usuario.id]);

        if (result.rows.length > 0) {
            res.redirect(`/painel?loja_id=${result.rows[0].id}`);
        } else {
            res.send("Seu usuário não possui nenhuma empresa vinculada.");
        }
    } catch (erro) {
        console.error(erro);
        res.send("Erro ao redirecionar.");
    }
});

// ==================================================================
// 6. ROTA DO PAINEL (COM PROTEÇÃO PARA LOJAS NOVAS)
// ==================================================================

app.get('/painel', authGuard, async (req, res) => {
    const lojaId = req.query.loja_id;
    const periodo = req.query.periodo || 'hoje';
    const { data_inicio, data_fim } = req.query;

    if (!lojaId) return res.redirect('/');

    try {
        // 1. Menu de Lojas
        const sqlLojas = `
            SELECT l.id, l.nome_fantasia 
            FROM lojas_sincronizadas l
            JOIN usuarios_lojas ul ON l.id = ul.loja_id
            WHERE ul.usuario_id = $1
            ORDER BY l.nome_fantasia ASC
        `;
        const todasLojas = await pool.query(sqlLojas, [req.session.usuario.id]);

        // 2. Info da Loja Atual
        const lojaRes = await pool.query(
            "SELECT schema_name, nome_fantasia FROM lojas_sincronizadas WHERE id = $1", 
            [lojaId]
        );
        
        if (lojaRes.rows.length === 0) return res.redirect('/');
        
        const schema = lojaRes.rows[0].schema_name;
        const nomeLoja = lojaRes.rows[0].nome_fantasia;

        // 3. Define Datas
        const hoje = new Date();
        let dIni = hoje.toISOString().split('T')[0];
        let dFim = hoje.toISOString().split('T')[0];

        if (periodo === 'personalizado' && data_inicio && data_fim) {
            dIni = data_inicio; dFim = data_fim;
        } else if (periodo === 'todos') {
            dIni = '2000-01-01';
        } else if (periodo === 'ontem') {
            const ontem = new Date(hoje); ontem.setDate(hoje.getDate() - 1);
            dIni = dFim = ontem.toISOString().split('T')[0];
        } else if (periodo === '7dias') {
            const d7 = new Date(hoje); d7.setDate(hoje.getDate() - 7);
            dIni = d7.toISOString().split('T')[0];
        } else if (periodo === '30dias') {
            const d30 = new Date(hoje); d30.setDate(hoje.getDate() - 30);
            dIni = d30.toISOString().split('T')[0];
        } else if (periodo === 'mes') {
            dIni = new Date(hoje.getFullYear(), hoje.getMonth(), 1).toISOString().split('T')[0];
        }

        // ============================================================
        // 🛡️ CORREÇÃO DE SEGURANÇA: Checa se a coluna 'total' existe
        // ============================================================
        const checkCol = await pool.query(`
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = 'saida' AND column_name = 'total'
        `, [schema]);

        // Se a coluna NÃO existe, define tudo como zero e renderiza sem erro
        if (checkCol.rows.length === 0) {
            return res.render('painel', { 
                dados: { 
                    faturamento: 0, qtde_vendas: 0, ticket_medio: 0, 
                    itens_por_venda: 0, cmv: 0, markup: 0, 
                    lucro_bruto: 0, margem: 0, maior_venda: 0, menor_venda: 0 
                }, 
                lojaId, nomeLoja, periodo, dIni, dFim, 
                todasLojas: todasLojas.rows, 
                usuario: req.session.usuario 
            });
        }
        // ============================================================

        // 4. Queries (COM CAST PARA NUMERIC PARA EVITAR ERRO DE SOMA)
        const sqlCapa = `
            SELECT 
                COALESCE(SUM(NULLIF("total", '')::numeric), 0) as faturamento,
                COUNT(*) as qtde_vendas,
                COALESCE(MAX(NULLIF("total", '')::numeric), 0) as maior_venda,
                COALESCE(MIN(NULLIF("total", '')::numeric), 0) as menor_venda
            FROM ${schema}.saida
            WHERE "data"::date BETWEEN $1 AND $2
        `;

        // Verifica se a tabela de produtos também tem as colunas (pra não dar erro no CMV)
        const checkItens = await pool.query(`
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = 'saida_produto' AND column_name = 'quant'
        `, [schema]);

        let resItens = { rows: [{ qtd_itens: 0, cmv: 0 }] };

        if (checkItens.rows.length > 0) {
            // CORREÇÃO: Tabela 'produto' no singular e CAST nos valores
            const sqlItens = `
                SELECT 
                    COALESCE(SUM(NULLIF(sp."quant", '')::numeric), 0) as qtd_itens,
                    COALESCE(SUM(NULLIF(sp."quant", '')::numeric * COALESCE(NULLIF(p.custo_total, '')::numeric, 0)), 0) as cmv
                FROM ${schema}.saida_produto sp
                JOIN ${schema}.saida s ON sp.id_saida = s.id_original
                LEFT JOIN ${schema}.produto p ON sp.id_produto = p.id_original
                WHERE s."data"::date BETWEEN $1 AND $2
            `;
            resItens = await pool.query(sqlItens, [dIni, dFim]);
        }

        const resCapa = await pool.query(sqlCapa, [dIni, dFim]);

        // 5. Cálculos
        const dados = { ...resCapa.rows[0], ...resItens.rows[0] };
        
        dados.faturamento = parseFloat(dados.faturamento);
        dados.cmv = parseFloat(dados.cmv);
        dados.qtde_vendas = parseInt(dados.qtde_vendas);
        dados.ticket_medio = dados.qtde_vendas > 0 ? (dados.faturamento / dados.qtde_vendas) : 0;
        dados.itens_por_venda = dados.qtde_vendas > 0 ? (dados.qtd_itens / dados.qtde_vendas) : 0;
        dados.lucro_bruto = dados.faturamento - dados.cmv;
        dados.markup = dados.cmv > 0 ? (dados.lucro_bruto / dados.cmv * 100) : 0;
        dados.margem = dados.faturamento > 0 ? (dados.lucro_bruto / dados.faturamento * 100) : 0;

        res.render('painel', { 
            dados, 
            lojaId, 
            nomeLoja, 
            periodo,
            dIni, dFim,
            todasLojas: todasLojas.rows, 
            usuario: req.session.usuario
        });

    } catch (erro) {
        console.error("Erro painel:", erro);
        res.send("Erro: " + erro.message);
    }
});

// ... Webhook de Cadastro e Inicialização (Mantém igual)
app.post('/webhook/criar-usuario', async (req, res) => {
    const { nome, telefone, cnpjs, admin_secret } = req.body;
    if (admin_secret !== SENHA_ADMIN_WEBHOOK) return res.status(401).json({ erro: "Senha incorreta." });
    if (!nome || !telefone || !cnpjs) return res.status(400).json({ erro: "Dados inválidos." });

    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const foneLimpo = telefone.replace(/\D/g, '');
        const userRes = await client.query(`
            INSERT INTO usuarios (nome, telefone) VALUES ($1, $2)
            ON CONFLICT (telefone) DO UPDATE SET nome = EXCLUDED.nome
            RETURNING id;
        `, [nome, foneLimpo]);
        const userId = userRes.rows[0].id;
        const lojasRes = await client.query(`SELECT id, cnpj FROM lojas_sincronizadas WHERE cnpj = ANY($1::text[])`, [cnpjs]);
        if (lojasRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ erro: "CNPJ não encontrado." }); }
        for (let loja of lojasRes.rows) {
            await client.query(`INSERT INTO usuarios_lojas (usuario_id, loja_id) VALUES ($1, $2) ON CONFLICT (usuario_id, loja_id) DO NOTHING`, [userId, loja.id]);
        }
        await client.query('COMMIT');
        res.json({ status: "sucesso", lojas: lojasRes.rows.map(l => l.cnpj) });
    } catch (e) { await client.query('ROLLBACK'); res.status(500).json({ erro: "Erro interno." }); } finally { client.release(); }
});

async function initDb() {
    let retries = 10;
    while (retries > 0) {
        try {
            const client = await pool.connect();
            try {
                await client.query(`CREATE TABLE IF NOT EXISTS usuarios (id SERIAL PRIMARY KEY, nome VARCHAR(100), telefone VARCHAR(20) UNIQUE, criado_em TIMESTAMP DEFAULT NOW());`);
                await client.query(`CREATE TABLE IF NOT EXISTS sessoes_login (id SERIAL PRIMARY KEY, telefone VARCHAR(20), token_acesso VARCHAR(6), expira_em TIMESTAMP, usado BOOLEAN DEFAULT FALSE);`);
                await client.query(`CREATE TABLE IF NOT EXISTS usuarios_lojas (usuario_id INT REFERENCES usuarios(id), loja_id INT REFERENCES lojas_sincronizadas(id), PRIMARY KEY (usuario_id, loja_id));`);
                console.log("✅ Banco OK");
            } finally { client.release(); }
            return;
        } catch (e) { console.log(`⏳ Banco... (${retries})`); retries--; await new Promise(r => setTimeout(r, 5000)); }
    }
    process.exit(1);
}

initDb().then(() => app.listen(PORT, () => console.log(`🚀 Front rodando: ${PORT}`)));