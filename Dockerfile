FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS final
WORKDIR /app
COPY ./publish/pg-webhook-relay-linux ./PgWebhookRelay
ENTRYPOINT ["./PgWebhookRelay"]