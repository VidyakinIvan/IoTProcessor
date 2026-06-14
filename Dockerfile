FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
RUN apt-get update && apt-get install -y apt-utils libc6 libstdc++6 libsnappy1v5 && ldconfig && rm -rf /var/li>
RUN mkdir -p /data/rocksdb
RUN ln -s /usr/lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so
COPY --from=build /app/publish /app
ENV LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:/app/runtimes/linux-x64/native:$LD_LIBRARY_PATH"
WORKDIR /app
ENTRYPOINT ["dotnet", "IoTProcessor.dll"]