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

// ==================================================================
// 2. UTILITÁRIOS & MIDDLEWARES
// ==================================================================

function gerarToken() { return Math.floor(100000 + Math.random() * 900000).toString(); }
function authGuard(req, res, next) { if (req.session.usuario) return next(); res.redirect('/login'); }
function filtroDataExe(periodo, data_inicio, data_fim) {
    const hoje = new Date();

    const formatarData = (data) => {
        const ano = data.getFullYear();
        const mes = String(data.getMonth() + 1).padStart(2, '0');
        const dia = String(data.getDate()).padStart(2, '0');
        return `${ano}-${mes}-${dia}`;
    };

    let dIni = formatarData(hoje);
    let dFim = formatarData(hoje);

    switch (periodo) {

        case 'personalizado':
            if (data_inicio && data_fim) {
                dIni = data_inicio;
                dFim = data_fim;
            }
            break;

        case 'ontem':
            const ontem = new Date(hoje);
            ontem.setDate(hoje.getDate() - 1);
            dIni = formatarData(ontem);
            dFim = formatarData(ontem);
            break;

        case '7dias':
            const fim7 = new Date(hoje);
            fim7.setDate(hoje.getDate() - 1);
            dFim = formatarData(fim7);

            const ini7 = new Date(hoje);
            ini7.setDate(hoje.getDate() - 7);
            dIni = formatarData(ini7);
            break;

        case '15dias':
            const fim15 = new Date(hoje);
            fim15.setDate(hoje.getDate() - 1);
            dFim = formatarData(fim15);

            const ini15 = new Date(hoje);
            ini15.setDate(hoje.getDate() - 15);
            dIni = formatarData(ini15);
            break;

        case '30dias':
            const fim30 = new Date(hoje);
            fim30.setDate(hoje.getDate() - 1);
            dFim = formatarData(fim30);

            const ini30 = new Date(hoje);
            ini30.setDate(hoje.getDate() - 30);
            dIni = formatarData(ini30);
            break;

        case 'mes':
            dIni = formatarData(new Date(hoje.getFullYear(), hoje.getMonth(), 1));
            dFim = formatarData(hoje);
            break;

        case 'mes_passado':
            dIni = formatarData(new Date(hoje.getFullYear(), hoje.getMonth() - 1, 1));
            dFim = formatarData(new Date(hoje.getFullYear(), hoje.getMonth(), 0));
            break;

        case '3meses':
            const fim90 = new Date(hoje);
            fim90.setDate(hoje.getDate() - 1);
            dFim = formatarData(fim90);

            const ini90 = new Date(hoje);
            ini90.setDate(hoje.getDate() - 90);
            dIni = formatarData(ini90);
            break;

        case '6meses':
            const fim180 = new Date(hoje);
            fim180.setDate(hoje.getDate() - 1);
            dFim = formatarData(fim180);

            const ini180 = new Date(hoje);
            ini180.setDate(hoje.getDate() - 180);
            dIni = formatarData(ini180);
            break;

        case 'este_ano':
            dIni = formatarData(new Date(hoje.getFullYear(), 0, 1));
            dFim = formatarData(hoje);
            break;

        case 'ano_passado':
            dIni = formatarData(new Date(hoje.getFullYear() - 1, 0, 1));
            dFim = formatarData(new Date(hoje.getFullYear() - 1, 11, 31));
            break;

        case 'hoje':
        default:
            dIni = formatarData(hoje);
            dFim = formatarData(hoje);
            break;
    }

    // console.log('Periodo: ', periodo);
    // console.log('Data Inicial: ', dIni);
    // console.log('Data Final: ', dFim);
    // console.log('==================================================================');

    return {
        data_inicio: dIni,
        data_fim: dFim,
    };
}
// ==================================================================
// 3. ROTAS DE AUTENTICAÇÃO
// ==================================================================

app.get('/login', (req, res) => { res.render('login', { erro: null }); });

app.post('/auth/solicitar', async (req, res) => {
    const { telefone } = req.body;
    const foneLimpo = telefone.replace(/\D/g, '');
    if (foneLimpo.length < 8) return res.render('login', { erro: "Número inválido." });
    try {
        let querySQL = "SELECT * FROM usuarios WHERE telefone LIKE $1";
        let params = [`%${foneLimpo}`];
        if (foneLimpo.length >= 10) {
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

app.get('/verificar', (req, res) => {
    try {
        if (!req.session.temp_telefone) return res.redirect('/login');
        const horaServidor = new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo', hour12: false });
        res.render('verificar', { telefone: req.session.temp_telefone, erro: null, horaServidor });
    } catch (erro) {
        console.error(erro);

        res.status(500).render('error', {
            erro: erro,
            message: "Não foi possível carregar a tela de verificação, tente novamente mais tarde ou entre em contato com o suporte."
        });
    }
});

app.post('/auth/validar', async (req, res) => {
    const { codigo } = req.body;
    const telefone = req.session.temp_telefone;
    try {
        const checkResult = await pool.query("SELECT * FROM sessoes_login WHERE telefone = $1 AND token_acesso = $2 AND usado = FALSE", [telefone, codigo]);
        if (checkResult.rows.length === 0) return res.render('verificar', { telefone, erro: "Código inválido.", horaServidor: new Date().toLocaleString() });

        const sessao = checkResult.rows[0];
        if (new Date() > new Date(sessao.expira_em)) return res.render('verificar', { telefone, erro: "Código expirado.", horaServidor: new Date().toLocaleString() });

        await pool.query("UPDATE sessoes_login SET usado = TRUE WHERE id = $1", [sessao.id]);
        const user = await pool.query("SELECT * FROM usuarios WHERE telefone = $1", [telefone]);
        req.session.usuario = user.rows[0];
        res.redirect('/');
    } catch (erro) { console.error(erro); res.redirect('/login'); }
});

app.get('/logout', (req, res) => { req.session.destroy(); res.redirect('/login'); });

// ==================================================================
// 4. ROTAS DO DASHBOARD (O BACKEND PROCESSA OS DADOS)
// ==================================================================

app.get('/', authGuard, async (req, res) => {
    try {
        // CORREÇÃO: Adicionado "AND l.ativo = TRUE" para ignorar empresas desativadas
        const query = `
            SELECT l.id 
            FROM lojas_sincronizadas l 
            JOIN usuarios_lojas ul ON l.id = ul.loja_id 
            WHERE ul.usuario_id = $1 AND l.ativo = TRUE 
            ORDER BY l.id ASC LIMIT 1
        `;
        const result = await pool.query(query, [req.session.usuario.id]);

        if (result.rows.length > 0) {
            res.redirect(`/painel?loja_id=${result.rows[0].id}`);
        } else {
            res.status(403).render('error', {
                erro: null,
                message: "Nenhuma empresa ativa vinculada ao seu usuário. Entre em contato com o suporte."
            });
        }
    } catch (erro) {
        console.error(erro);
        res.status(500).render('error', {
            erro: erro,
            message: "Não foi possível carregar o painel inicial."
        });
    }
});

app.get('/painel', authGuard, async (req, res) => {
    const lojaId = req.query.loja_id;
    const periodo = req.query.periodo || 'hoje';
    const { data_inicio, data_fim } = req.query;
    if (!lojaId) return res.redirect('/');

    try {
        // 1. Busca lista de lojas APENAS ATIVAS para o menu
        const todasLojas = await pool.query(`
            SELECT l.id, l.nome_fantasia 
            FROM lojas_sincronizadas l 
            JOIN usuarios_lojas ul ON l.id = ul.loja_id 
            WHERE ul.usuario_id = $1 AND l.ativo = TRUE 
            ORDER BY l.nome_fantasia ASC
        `, [req.session.usuario.id]);

        // 2. Verifica se a loja atual é válida e está ativa
        const lojaRes = await pool.query("SELECT api_token, nome_fantasia, ativo FROM lojas_sincronizadas WHERE id = $1", [lojaId]);
        
        // Se não achar a loja ou ela estiver inativa, joga o usuário de volta para o início (que vai achar uma ativa)
        if (lojaRes.rows.length === 0 || !lojaRes.rows[0].ativo) return res.redirect('/');

        const { api_token, nome_fantasia: nomeLoja } = lojaRes.rows[0];

        const filtroData = filtroDataExe(periodo, data_inicio, data_fim);
        dIni = filtroData.data_inicio;
        dFim = filtroData.data_fim;

        const urlApi = `${API_PYTHON_URL}/reports/dashboard-cards?data_inicio=${dIni}&data_fim=${dFim}`;
        const respostaApi = await axios.get(urlApi, {
            headers: { 'Authorization': `Bearer ${api_token}` }
        });

        res.render('relatorio', {
            modo: 'painel',
            dados: respostaApi.data,
            lojaId, nomeLoja, periodo, dIni, dFim, todasLojas: todasLojas.rows, usuario: req.session.usuario
        });

    } catch (erro) {
        console.error(erro);
        res.status(500).render('error', { erro: erro, message: "Erro ao carregar painel." });
    }
});

app.get('/relatorios/:tipo', authGuard, async (req, res) => {
    const { tipo } = req.params;
    const { loja_id: lojaId, periodo, data_inicio, data_fim } = req.query;

    if (!lojaId) return res.redirect('/');

    // ... (Mantenha o objeto 'titulos' igual ao original aqui) ...
    const titulos = { 'produto': 'Produtos', 'hora': 'Horários', 'dia': 'Dias', 'pagamento': 'Pagamentos', 'secao': 'Seção', 'grupo': 'Grupo', 'fabricante': 'Fabricante', 'fornecedor': 'Fornecedor', 'cliente': 'Clientes', 'terminal': 'Terminal', 'usuario': 'Usuário', 'vendedor': 'Vendedor' };

    if (!titulos[tipo]) return res.status(400).render('error', { erro: null, message: `Tipo inválido: ${tipo}` });

    try {
        const [lojaRes, todasLojas] = await Promise.all([
            // Verifica ativo na loja atual
            pool.query("SELECT id, api_token, nome_fantasia, ativo FROM lojas_sincronizadas WHERE id = $1", [lojaId]),
            // Filtra ativas na lista geral
            pool.query(`
                SELECT l.id, l.nome_fantasia 
                FROM lojas_sincronizadas l 
                JOIN usuarios_lojas ul ON l.id = ul.loja_id 
                WHERE ul.usuario_id = $1 AND l.ativo = TRUE 
                ORDER BY l.nome_fantasia ASC
            `, [req.session.usuario.id])
        ]);

        // Se loja inativa ou inexistente -> redireciona para Home
        if (lojaRes.rows.length === 0 || !lojaRes.rows[0].ativo) return res.redirect('/');

        const lojaAtual = lojaRes.rows[0];
        const filtroData = filtroDataExe(periodo, data_inicio, data_fim);
        const urlApi = `${API_PYTHON_URL}/reports/ranking/${tipo}?data_inicio=${filtroData.data_inicio}&data_fim=${filtroData.data_fim}&limit=50`;

        const respostaApi = await axios.get(urlApi, {
            headers: { 'Authorization': `Bearer ${lojaAtual.api_token}` }
        });

        res.render('relatorio', {
            tipo, tituloRelatorio: titulos[tipo], dados: respostaApi.data,
            lojaId, lojaAtual, periodo: periodo || 'hoje',
            dIni: filtroData.data_inicio, dFim: filtroData.data_fim,
            todasLojas: todasLojas.rows, usuario: req.session.usuario
        });

    } catch (erro) {
        console.error(`Erro relatório ${tipo}:`, erro.message);
        res.status(500).render('error', { erro: erro, message: "Erro ao gerar relatório." });
    }
});
// ==================================================================
// 5. WEBHOOKS & INICIALIZAÇÃO
// ==================================================================

app.post('/webhook/criar-usuario', async (req, res) => {
    const { nome, telefone, cnpjs, admin_secret } = req.body;
    if (admin_secret !== SENHA_ADMIN_WEBHOOK) return res.status(401).json({ erro: "Não autorizado" });
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const foneLimpo = telefone.replace(/\D/g, '');
        const userRes = await client.query(`INSERT INTO usuarios (nome, telefone) VALUES ($1, $2) ON CONFLICT (telefone) DO UPDATE SET nome = EXCLUDED.nome RETURNING id;`, [nome, foneLimpo]);
        const userId = userRes.rows[0].id;
        const lojasRes = await client.query(`SELECT id FROM lojas_sincronizadas WHERE cnpj = ANY($1::text[])`, [cnpjs]);
        for (let loja of lojasRes.rows) { await client.query(`INSERT INTO usuarios_lojas (usuario_id, loja_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [userId, loja.id]); }
        await client.query('COMMIT'); res.json({ status: "sucesso" });
    } catch (e) { await client.query('ROLLBACK'); res.status(500).json({ erro: "Erro interno" }); } finally { client.release(); }
});

async function initDb() {
    try {
        const client = await pool.connect();
        await client.query(`CREATE TABLE IF NOT EXISTS lojas_sincronizadas (id SERIAL PRIMARY KEY, nome_fantasia VARCHAR(100), cnpj VARCHAR(20) UNIQUE, api_token VARCHAR(100), schema_name VARCHAR(50) NOT NULL, ativo BOOLEAN DEFAULT TRUE, criado_em TIMESTAMP DEFAULT NOW());`);
        await client.query(`CREATE TABLE IF NOT EXISTS usuarios (id SERIAL PRIMARY KEY, nome VARCHAR(100), telefone VARCHAR(20) UNIQUE, criado_em TIMESTAMP DEFAULT NOW());`);
        await client.query(`CREATE TABLE IF NOT EXISTS sessoes_login (id SERIAL PRIMARY KEY, telefone VARCHAR(20), token_acesso VARCHAR(6), expira_em TIMESTAMP, usado BOOLEAN DEFAULT FALSE);`);
        await client.query(`CREATE TABLE IF NOT EXISTS usuarios_lojas (usuario_id INT REFERENCES usuarios(id), loja_id INT REFERENCES lojas_sincronizadas(id), PRIMARY KEY (usuario_id, loja_id));`);
        client.release();
        console.log("✅ Banco OK");
    } catch (e) { console.error("❌ Erro Banco:", e.message); setTimeout(initDb, 5000); }
}



initDb().then(() => app.listen(PORT, () => console.log(`🚀 Front rodando na porta ${PORT}`)));