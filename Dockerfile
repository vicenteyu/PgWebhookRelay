FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS final
WORKDIR /app
COPY ./bin/Release/net10.0/publish/linux-x64/PgWebhookRelay .
ENTRYPOINT ["./PgWebhookRelay"]