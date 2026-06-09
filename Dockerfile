FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish "IoTProcessor.csproj" -c Release -o /app/publish

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libc6 libstdc++6 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/publish /app
WORKDIR /app
ENTRYPOINT ["dotnet", "IoTProcessor.dll"]