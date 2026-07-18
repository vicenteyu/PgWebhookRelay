FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS final
WORKDIR /app
RUN apt-get update && apt-get install -y libgssapi-krb5-2 && rm -rf /var/lib/apt/lists/*
COPY --chmod=755 ./publish/pg-webhook-relay-linux ./PgWebhookRelay
ENTRYPOINT ["./PgWebhookRelay"]