# Usa uma imagem leve do Node.js
FROM node:18-alpine

# Cria a pasta de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de dependências primeiro (aproveita cache do Docker)
COPY package*.json ./

# Instala as dependências
RUN npm install --production

# Copia o restante do código (server.js, views/, public/, etc)
COPY . .

# Expõe a porta 3000
EXPOSE 3000

# Comando para iniciar
CMD ["node", "server.js"]