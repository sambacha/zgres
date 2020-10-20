
FROM alpine:10

RUN apk add --update --no-cache nodejs
RUN npm i -g yarn

ADD package.json yarn.lock /tmp/
ADD .yarn-cache.tgz /

RUN cd /tmp && yarn
RUN mkdir -p /service && cd /service && ln -s /tmp/node_modules

COPY . /service
WORKDIR /service

ENV FORCE_COLOR=1

ENTRYPOINT ["npm"]
CMD ["start"]
