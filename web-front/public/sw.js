const CACHE_NAME = 'bm-dashboard-v1';
const ASSETS_TO_CACHE = [
  '/login',
  '/assets/vendor/css/core.css',
  '/assets/vendor/css/pages/page-auth.css',
  '/assets/vendor/js/helpers.js',
  '/assets/js/config.js',
  '/assets/img/icons/logo.svg',
  '/assets/vendor/js/bootstrap.js',
  '/assets/vendor/libs/jquery/jquery.js'
];

// Instalação do Service Worker e cache dos arquivos estáticos
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(ASSETS_TO_CACHE);
    })
  );
});

// Interceptação de requisições (Estratégia: Network First, fallback to Cache)
self.addEventListener('fetch', (event) => {
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        // Se a resposta for válida, clone e atualize o cache (opcional para páginas dinâmicas)
        return response;
      })
      .catch(() => {
        // Se falhar (offline), tenta buscar no cache
        return caches.match(event.request);
      })
  );
});