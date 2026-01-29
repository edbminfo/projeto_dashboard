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

// >>>> ALTERAÇÃO AQUI: URL agora vem do Docker Compose <<<<
const API_PYTHON_URL = process.env.API_INTERNAL_URL || "http://api-dashboard:8000/api";

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

// ... [O RESTO DO ARQUIVO PERMANECE IDÊNTICO AO ANTERIOR] ...
// (Mantenha todo o código abaixo: middlewares, rotas de login, painel, webhook e initDb)

app.use(bodyParser.urlencoded({ extended: true })); 
app.use(bodyParser.json()); 
app.use(express.static('public')); 
app.set('view engine', 'ejs'); 

app.use(session({
    secret: 'segredo-super-seguro-bmhelp',
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false, httpOnly: true, maxAge: 1000 * 60 * 60 * 24 * 30 }
}));

function gerarToken() { return Math.floor(100000 + Math.random() * 900000).toString(); }
function authGuard(req, res, next) { if (req.session.usuario) return next(); res.redirect('/login'); }

app.get('/login', (req, res) => { res.render('login', { erro: null }); });
app.post('/auth/solicitar', async (req, res) => {
    const { telefone } = req.body;
    const foneLimpo = telefone.replace(/\D/g, '');
    if (foneLimpo.length < 8) return res.render('login', { erro: "Número inválido." });
    try {
        let querySQL = "SELECT * FROM usuarios WHERE telefone LIKE $1";
        let params = [`%${foneLimpo}`];
        if (foneLimpo.length >= 10) {
            querySQL = "SELECT * FROM usuarios WHERE telefone LIKE $1";
            params = [`%${foneLimpo.substring(0, 2)}%${foneLimpo.slice(-8)}`]; 
        }
        const userCheck = await pool.query(querySQL, params);
        if (userCheck.rows.length === 0) return res.render('login', { erro: "Usuário não encontrado." });
        const telefoneOficial = userCheck.rows[0].telefone; 
        const tokenAcesso = gerarToken();
        const expira = new Date(Date.now() + 5 * 60000);
        await pool.query("INSERT INTO sessoes_login (telefone, token_acesso, expira_em) VALUES ($1, $2, $3)", [telefoneOficial, tokenAcesso, expira]);
        await axios.post(N8N_WEBHOOK_URL, {
            "token": N8N_TOKEN_AUTH, "tipo": "text", "instancia": INSTANCIA_WHATS, "telefone": telefoneOficial,
            "cont": `Seu código de acesso ao Dashboard BM: *${tokenAcesso}*`
        });
        req.session.temp_telefone = telefoneOficial;
        res.redirect('/verificar');
    } catch (erro) { console.error("Erro no login:", erro); res.render('login', { erro: "Erro ao processar login." }); }
});

// ... (seu código anterior)

// --- ROTA GET: TELA DE VERIFICAÇÃO ---
app.get('/verificar', (req, res) => {
    if (!req.session.temp_telefone) return res.redirect('/login');
    
    // Formata a hora atual do servidor para mostrar na tela
    const horaServidor = new Date().toLocaleString('pt-BR', { 
        timeZone: 'America/Sao_Paulo',
        hour12: false,
        hour: '2-digit', minute: '2-digit', second: '2-digit'
    });

    console.log(`[DEBUG] Acessando /verificar. Hora do Node: ${new Date().toISOString()} | Formatada: ${horaServidor}`);

    res.render('verificar', { 
        telefone: req.session.temp_telefone, 
        erro: null,
        horaServidor: horaServidor // Enviando para o front
    });
});

// --- ROTA POST: VALIDAR CÓDIGO ---
app.post('/auth/validar', async (req, res) => {
    const { codigo } = req.body;
    const telefone = req.session.temp_telefone;

    try {
        // Log para entender o que está acontecendo
        console.log(`[LOGIN] Tentativa: ${telefone} com código ${codigo}`);
        
        // Query de verificação com log
        const checkQuery = "SELECT * FROM sessoes_login WHERE telefone = $1 AND token_acesso = $2 AND usado = FALSE";
        const checkResult = await pool.query(checkQuery, [telefone, codigo]);

        if (checkResult.rows.length === 0) {
            console.log("[LOGIN] Falha: Código não encontrado ou já usado.");
            return res.render('verificar', { telefone, erro: "Código inválido ou inexistente.", horaServidor: new Date().toLocaleString() });
        }

        const sessao = checkResult.rows[0];
        const agora = new Date(); // Hora do Node (com Fuso ajustado pelo Docker)
        const expira = new Date(sessao.expira_em); // Hora que veio do Banco

        console.log(`[LOGIN] Comparação de Tempo:`);
        console.log(`   - Agora (Node):   ${agora.toLocaleString('pt-BR')}`);
        console.log(`   - Expira (Banco): ${expira.toLocaleString('pt-BR')}`);

        // Validação manual de tempo para garantir
        if (agora > expira) {
            console.log("[LOGIN] Falha: Código expirado.");
            return res.render('verificar', { telefone, erro: "O código expirou. Tente novamente.", horaServidor: agora.toLocaleString('pt-BR') });
        }

        // Se passou, atualiza e loga
        await pool.query("UPDATE sessoes_login SET usado = TRUE WHERE id = $1", [sessao.id]);
        const user = await pool.query("SELECT * FROM usuarios WHERE telefone = $1", [telefone]);
        req.session.usuario = user.rows[0];
        console.log("[LOGIN] Sucesso! Redirecionando.");
        res.redirect('/');

    } catch (erro) {
        console.error("[ERRO FATAL LOGIN]", erro);
        res.redirect('/login');
    }
});

// ... (resto do código)

app.get('/logout', (req, res) => { req.session.destroy(); res.redirect('/login'); });

app.get('/', authGuard, async (req, res) => {
    try {
        const query = `SELECT l.id FROM lojas_sincronizadas l JOIN usuarios_lojas ul ON l.id = ul.loja_id WHERE ul.usuario_id = $1 ORDER BY l.id ASC LIMIT 1`;
        const result = await pool.query(query, [req.session.usuario.id]);
        if (result.rows.length > 0) res.redirect(`/painel?loja_id=${result.rows[0].id}`);
        else res.send("Seu usuário não possui nenhuma empresa vinculada.");
    } catch (erro) { console.error(erro); res.send("Erro ao redirecionar."); }
});

app.get('/painel', authGuard, async (req, res) => {
    const lojaId = req.query.loja_id;
    const periodo = req.query.periodo || 'hoje';
    const { data_inicio, data_fim } = req.query;
    if (!lojaId) return res.redirect('/');

    try {
        const todasLojas = await pool.query(`SELECT l.id, l.nome_fantasia FROM lojas_sincronizadas l JOIN usuarios_lojas ul ON l.id = ul.loja_id WHERE ul.usuario_id = $1 ORDER BY l.nome_fantasia ASC`, [req.session.usuario.id]);
        const lojaRes = await pool.query("SELECT schema_name, nome_fantasia FROM lojas_sincronizadas WHERE id = $1", [lojaId]);
        if (lojaRes.rows.length === 0) return res.redirect('/');
        const { schema_name: schema, nome_fantasia: nomeLoja } = lojaRes.rows[0];

        const hoje = new Date();
        let dIni = hoje.toISOString().split('T')[0];
        let dFim = hoje.toISOString().split('T')[0];
        if (periodo === 'personalizado' && data_inicio && data_fim) { dIni = data_inicio; dFim = data_fim; }
        else if (periodo === 'todos') { dIni = '2000-01-01'; }
        else if (periodo === 'ontem') { const ontem = new Date(hoje); ontem.setDate(hoje.getDate() - 1); dIni = dFim = ontem.toISOString().split('T')[0]; }
        else if (periodo === '7dias') { const d7 = new Date(hoje); d7.setDate(hoje.getDate() - 7); dIni = d7.toISOString().split('T')[0]; }
        else if (periodo === 'mes') { dIni = new Date(hoje.getFullYear(), hoje.getMonth(), 1).toISOString().split('T')[0]; }

        const checkCol = await pool.query(`SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'saida' AND column_name = 'total'`, [schema]);
        if (checkCol.rows.length === 0) {
            return res.render('painel', { dados: { faturamento: 0, qtde_vendas: 0, ticket_medio: 0, itens_por_venda: 0, cmv: 0, markup: 0, lucro_bruto: 0, margem: 0, maior_venda: 0, menor_venda: 0 }, lojaId, nomeLoja, periodo, dIni, dFim, todasLojas: todasLojas.rows, usuario: req.session.usuario });
        }

        const sqlCapa = `SELECT COALESCE(SUM(NULLIF("total", '')::numeric), 0) as faturamento, COUNT(*) as qtde_vendas, COALESCE(MAX(NULLIF("total", '')::numeric), 0) as maior_venda, COALESCE(MIN(NULLIF("total", '')::numeric), 0) as menor_venda FROM ${schema}.saida WHERE "data"::date BETWEEN $1 AND $2 AND (eliminado IS NULL OR eliminado = 'N')`;
        
        let resItens = { rows: [{ qtd_itens: 0, cmv: 0 }] };
        const checkItens = await pool.query(`SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'saida_produto' AND column_name = 'quant'`, [schema]);
        if (checkItens.rows.length > 0) {
            const sqlItens = `SELECT COALESCE(SUM(NULLIF(sp."quant", '')::numeric), 0) as qtd_itens, COALESCE(SUM(NULLIF(sp."quant", '')::numeric * COALESCE(NULLIF(p.custo_total, '')::numeric, 0)), 0) as cmv FROM ${schema}.saida_produto sp JOIN ${schema}.saida s ON sp.id_saida = s.id_original LEFT JOIN ${schema}.produto p ON sp.id_produto = p.id_original WHERE s."data"::date BETWEEN $1 AND $2 AND (s.eliminado IS NULL OR s.eliminado = 'N')`;
            resItens = await pool.query(sqlItens, [dIni, dFim]);
        }
        const resCapa = await pool.query(sqlCapa, [dIni, dFim]);

        const dados = { ...resCapa.rows[0], ...resItens.rows[0] };
        dados.faturamento = parseFloat(dados.faturamento);
        dados.cmv = parseFloat(dados.cmv);
        dados.qtde_vendas = parseInt(dados.qtde_vendas);
        dados.ticket_medio = dados.qtde_vendas > 0 ? (dados.faturamento / dados.qtde_vendas) : 0;
        dados.itens_por_venda = dados.qtde_vendas > 0 ? (dados.qtd_itens / dados.qtde_vendas) : 0;
        dados.lucro_bruto = dados.faturamento - dados.cmv;
        dados.markup = dados.cmv > 0 ? (dados.lucro_bruto / dados.cmv * 100) : 0;
        dados.margem = dados.faturamento > 0 ? (dados.lucro_bruto / dados.faturamento * 100) : 0;

        res.render('painel', { dados, lojaId, nomeLoja, periodo, dIni, dFim, todasLojas: todasLojas.rows, usuario: req.session.usuario });

    } catch (erro) { console.error("Erro painel:", erro); res.send("Erro: " + erro.message); }
});

app.get('/relatorios/:tipo', authGuard, async (req, res) => {
    const { tipo } = req.params;
    const lojaId = req.query.loja_id;
    const periodo = req.query.periodo || 'hoje';
    const { data_inicio, data_fim } = req.query;

    if (!lojaId) return res.redirect('/');

    try {
        const lojaRes = await pool.query("SELECT api_token, nome_fantasia FROM lojas_sincronizadas WHERE id = $1", [lojaId]);
        if (lojaRes.rows.length === 0) return res.redirect('/');
        const { api_token, nome_fantasia: nomeLoja } = lojaRes.rows[0];
        
        const todasLojas = await pool.query(`SELECT l.id, l.nome_fantasia FROM lojas_sincronizadas l JOIN usuarios_lojas ul ON l.id = ul.loja_id WHERE ul.usuario_id = $1 ORDER BY l.nome_fantasia ASC`, [req.session.usuario.id]);

        const hoje = new Date();
        let dIni = hoje.toISOString().split('T')[0];
        let dFim = hoje.toISOString().split('T')[0];
        if (periodo === 'personalizado' && data_inicio && data_fim) { dIni = data_inicio; dFim = data_fim; }
        else if (periodo === 'todos') { dIni = '2000-01-01'; }
        else if (periodo === 'ontem') { const ontem = new Date(hoje); ontem.setDate(hoje.getDate() - 1); dIni = dFim = ontem.toISOString().split('T')[0]; }
        else if (periodo === '7dias') { const d7 = new Date(hoje); d7.setDate(hoje.getDate() - 7); dIni = d7.toISOString().split('T')[0]; }
        else if (periodo === 'mes') { dIni = new Date(hoje.getFullYear(), hoje.getMonth(), 1).toISOString().split('T')[0]; }

        try {
            // USANDO A CONSTANTE QUE PEGA DO ENV
            const urlApi = `${API_PYTHON_URL}/reports/ranking/${tipo}?data_inicio=${dIni}&data_fim=${dFim}&limit=50`;
            const respostaApi = await axios.get(urlApi, {
                headers: { 'Authorization': `Bearer ${api_token}` }
            });
            const dadosRelatorio = respostaApi.data;

            const titulos = {
                'produto': 'Produtos mais Vendidos', 'hora': 'Vendas por Hora', 'dia': 'Vendas por Dia',
                'pagamento': 'Vendas por Pagamento', 'secao': 'Vendas por Seção', 'grupo': 'Vendas por Grupo',
                'fabricante': 'Vendas por Fabricante', 'fornecedor': 'Vendas por Fornecedor', 'cliente': 'Principais Clientes',
                'terminal': 'Vendas por Terminal', 'usuario': 'Vendas por Usuário', 'vendedor': 'Vendas por Vendedor'
            };

            res.render('relatorio', { 
                tipo,
                tituloRelatorio: titulos[tipo] || 'Relatório',
                dados: dadosRelatorio, 
                lojaId, nomeLoja, periodo, dIni, dFim, 
                todasLojas: todasLojas.rows, 
                usuario: req.session.usuario 
            });

        } catch (apiErro) {
            console.error("Erro API Python:", apiErro.message);
            res.send("Erro ao buscar dados do relatório: " + apiErro.message);
        }

    } catch (erro) { console.error("Erro rota relatorio:", erro); res.send("Erro geral: " + erro.message); }
});

app.post('/webhook/criar-usuario', async (req, res) => {
    const { nome, telefone, cnpjs, admin_secret } = req.body;
    if (admin_secret !== SENHA_ADMIN_WEBHOOK) return res.status(401).json({ erro: "Senha incorreta." });
    if (!nome || !telefone || !cnpjs) return res.status(400).json({ erro: "Dados inválidos." });
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const foneLimpo = telefone.replace(/\D/g, '');
        const userRes = await client.query(`INSERT INTO usuarios (nome, telefone) VALUES ($1, $2) ON CONFLICT (telefone) DO UPDATE SET nome = EXCLUDED.nome RETURNING id;`, [nome, foneLimpo]);
        const userId = userRes.rows[0].id;
        const lojasRes = await client.query(`SELECT id, cnpj FROM lojas_sincronizadas WHERE cnpj = ANY($1::text[])`, [cnpjs]);
        if (lojasRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ erro: "CNPJ não encontrado." }); }
        for (let loja of lojasRes.rows) { await client.query(`INSERT INTO usuarios_lojas (usuario_id, loja_id) VALUES ($1, $2) ON CONFLICT (usuario_id, loja_id) DO NOTHING`, [userId, loja.id]); }
        await client.query('COMMIT'); res.json({ status: "sucesso", lojas: lojasRes.rows.map(l => l.cnpj) });
    } catch (e) { await client.query('ROLLBACK'); res.status(500).json({ erro: "Erro interno." }); } finally { client.release(); }
});

async function initDb() {
    let retries = 10;
    while (retries > 0) {
        try {
            const client = await pool.connect();
            try {
                await client.query(`CREATE TABLE IF NOT EXISTS lojas_sincronizadas (id SERIAL PRIMARY KEY, nome_fantasia VARCHAR(100), cnpj VARCHAR(20) UNIQUE, api_token VARCHAR(100), schema_name VARCHAR(50) NOT NULL, ativo BOOLEAN DEFAULT TRUE, criado_em TIMESTAMP DEFAULT NOW());`);
                await client.query(`CREATE TABLE IF NOT EXISTS usuarios (id SERIAL PRIMARY KEY, nome VARCHAR(100), telefone VARCHAR(20) UNIQUE, criado_em TIMESTAMP DEFAULT NOW());`);
                await client.query(`CREATE TABLE IF NOT EXISTS sessoes_login (id SERIAL PRIMARY KEY, telefone VARCHAR(20), token_acesso VARCHAR(6), expira_em TIMESTAMP, usado BOOLEAN DEFAULT FALSE);`);
                await client.query(`CREATE TABLE IF NOT EXISTS usuarios_lojas (usuario_id INT REFERENCES usuarios(id), loja_id INT REFERENCES lojas_sincronizadas(id), PRIMARY KEY (usuario_id, loja_id));`);
                console.log("✅ Banco OK");
            } finally { client.release(); } return;
        } catch (e) { console.log(`⏳ Banco... (${retries})`); retries--; await new Promise(r => setTimeout(r, 5000)); }
    }
    process.exit(1);
}
initDb().then(() => app.listen(PORT, () => console.log(`🚀 Front rodando: ${PORT}`)));