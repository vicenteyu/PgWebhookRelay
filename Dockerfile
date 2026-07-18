FROM mcr.microsoft.com/dotnet/runtime:10.0-noble AS library-builder
RUN apt-get update && apt-get install -y libgssapi-krb5-2

FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS final
WORKDIR /app

COPY --from=library-builder /usr/lib/x86_64-linux-gnu/libgssapi_krb5.so.2 /usr/lib/x86_64-linux-gnu/
COPY --from=library-builder /usr/lib/x86_64-linux-gnu/libkrb5.so.3 /usr/lib/x86_64-linux-gnu/
COPY --from=library-builder /usr/lib/x86_64-linux-gnu/libk5crypto.so.3 /usr/lib/x86_64-linux-gnu/
COPY --from=library-builder /usr/lib/x86_64-linux-gnu/libcom_err.so.2 /usr/lib/x86_64-linux-gnu/
COPY --from=library-builder /usr/lib/x86_64-linux-gnu/libkrb5support.so.0 /usr/lib/x86_64-linux-gnu/

COPY --chmod=755 ./publish/pg-webhook-relay-linux ./PgWebhookRelay
ENTRYPOINT ["./PgWebhookRelay"]