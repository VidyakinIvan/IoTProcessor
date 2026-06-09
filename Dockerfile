FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
RUN apt-get update && apt-get install -y libc6 libstdc++6
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "IoTProcessor.dll"]