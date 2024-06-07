FROM node:18.16.0

WORKDIR /app

COPY frodo-app/package*.json ./frodo-app/

RUN cd frodo-app && npm install

COPY . .

WORKDIR /app/frodo-app

EXPOSE 3000

CMD ["npm", "start"]
