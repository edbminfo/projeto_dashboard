# Usa uma imagem leve do Node.js
FROM node:18-alpine

# Cria a pasta de trabalho dentro do container
WORKDIR /app

# Instala o suporte a fuso horário (tzdata)
RUN apk add --no-cache tzdata

# Copia os arquivos de dependências
COPY package*.json ./

# Instala as dependências
RUN npm install --production

# Copia o restante do código
COPY . .

# Expõe a porta 3000
EXPOSE 3000

# Comando para iniciar
CMD ["node", "server.js"]