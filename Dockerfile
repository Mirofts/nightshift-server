# Works on Fly.io, Railway, Koyeb, any Docker host: game + online matchmaking on one URL.
FROM node:20-alpine
WORKDIR /app
COPY . .
ENV PORT=8080
ENV STATIC=0
EXPOSE 8080
CMD ["node", "server/server.js"]
