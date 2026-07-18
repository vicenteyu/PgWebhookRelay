FROM mcr.microsoft.com/runtime-deps:10.0-jammy-chiseled

WORKDIR /app

COPY ./bin/Release/net10.0/publish/linux-x64/PgWebhookRelay .

ENTRYPOINT ["./PgWebhookRelay"]