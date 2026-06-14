FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
RUN apt-get update && apt-get install -y apt-utils libc6 libstdc++6 libsnappy1v5 && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /data/rocksdb
RUN ln -s /usr/lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so
COPY --from=build /app/publish /app
ENV LD_LIBRARY_PATH="/app/runtimes/linux-x64/native:/usr/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH"
WORKDIR /app
ENTRYPOINT ["dotnet", "IoTProcessor.dll"]