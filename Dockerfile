FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:10.0
RUN apt-get update && apt-get install -y libc6 libstdc++6 && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so
COPY --from=build /app/publish /app
WORKDIR /app
ENTRYPOINT ["dotnet", "IoTProcessor.dll"]