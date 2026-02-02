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
    if (!req.session.temp_telefone) return res.redirect('/login');
    const horaServidor = new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo', hour12: false });
    res.render('verificar', { telefone: req.session.temp_telefone, erro: null, horaServidor });
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
        const query = `SELECT l.id FROM lojas_sincronizadas l JOIN usuarios_lojas ul ON l.id = ul.loja_id WHERE ul.usuario_id = $1 ORDER BY l.id ASC LIMIT 1`;
        const result = await pool.query(query, [req.session.usuario.id]);
        if (result.rows.length > 0) res.redirect(`/painel?loja_id=${result.rows[0].id}`);
        else res.send("Nenhuma empresa vinculada.");
    } catch (erro) { res.send("Erro ao redirecionar."); }
});

app.get('/painel', authGuard, async (req, res) => {
    const lojaId = req.query.loja_id;
    const periodo = req.query.periodo || 'hoje';
    const { data_inicio, data_fim } = req.query;
    if (!lojaId) return res.redirect('/');

    try {
        const todasLojas = await pool.query(`SELECT l.id, l.nome_fantasia FROM lojas_sincronizadas l JOIN usuarios_lojas ul ON l.id = ul.loja_id WHERE ul.usuario_id = $1 ORDER BY l.nome_fantasia ASC`, [req.session.usuario.id]);
        const lojaRes = await pool.query("SELECT api_token, nome_fantasia FROM lojas_sincronizadas WHERE id = $1", [lojaId]);
        if (lojaRes.rows.length === 0) return res.redirect('/');
        
        const { api_token, nome_fantasia: nomeLoja } = lojaRes.rows[0];

        // Lógica de Datas
        const hoje = new Date();
        let dIni = hoje.toISOString().split('T')[0];
        let dFim = hoje.toISOString().split('T')[0];
        if (periodo === 'personalizado' && data_inicio && data_fim) { dIni = data_inicio; dFim = data_fim; }
        else if (periodo === 'ontem') { const ontem = new Date(hoje); ontem.setDate(hoje.getDate() - 1); dIni = dFim = ontem.toISOString().split('T')[0]; }
        else if (periodo === '7dias') { const d7 = new Date(hoje); d7.setDate(hoje.getDate() - 7); dIni = d7.toISOString().split('T')[0]; }
        else if (periodo === 'mes') { dIni = new Date(hoje.getFullYear(), hoje.getMonth(), 1).toISOString().split('T')[0]; }

        // CHAMADA AO BACKEND PYTHON PARA OBTER OS CARDS CALCULADOS
        const urlApi = `${API_PYTHON_URL}/reports/dashboard-cards?data_inicio=${dIni}&data_fim=${dFim}`;
        const respostaApi = await axios.get(urlApi, {
            headers: { 'Authorization': `Bearer ${api_token}` }
        });

        res.render('painel', { 
            dados: respostaApi.data, // Dados calculados integralmente pelo Python
            lojaId, nomeLoja, periodo, dIni, dFim, todasLojas: todasLojas.rows, usuario: req.session.usuario 
        });

    } catch (erro) { console.error("Erro painel:", erro.message); res.status(500).send("Erro ao carregar indicadores."); }
});

app.get('/relatorios/:tipo', authGuard, async (req, res) => {
    const { tipo } = req.params;
    const { loja_id: lojaId, periodo = 'hoje', data_inicio, data_fim } = req.query;
    if (!lojaId) return res.redirect('/');

    try {
        const lojaRes = await pool.query("SELECT api_token, nome_fantasia FROM lojas_sincronizadas WHERE id = $1", [lojaId]);
        const { api_token, nome_fantasia: nomeLoja } = lojaRes.rows[0];
        const todasLojas = await pool.query(`SELECT l.id, l.nome_fantasia FROM lojas_sincronizadas l JOIN usuarios_lojas ul ON l.id = ul.loja_id WHERE ul.usuario_id = $1 ORDER BY l.nome_fantasia ASC`, [req.session.usuario.id]);

        let dIni = new Date().toISOString().split('T')[0];
        let dFim = dIni;
        if (periodo === 'personalizado' && data_inicio && data_fim) { dIni = data_inicio; dFim = data_fim; }

        const urlApi = `${API_PYTHON_URL}/reports/ranking/${tipo}?data_inicio=${dIni}&data_fim=${dFim}&limit=50`;
        const respostaApi = await axios.get(urlApi, { headers: { 'Authorization': `Bearer ${api_token}` } });

        const titulos = { 'produto': 'Produtos', 'hora': 'Horários', 'dia': 'Dias', 'pagamento': 'Pagamentos' };

        res.render('relatorio', { 
            tipo, tituloRelatorio: titulos[tipo] || 'Relatório', dados: respostaApi.data, 
            lojaId, nomeLoja, periodo, dIni, dFim, todasLojas: todasLojas.rows, usuario: req.session.usuario 
        });
    } catch (erro) { res.status(500).send("Erro ao carregar relatório."); }
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