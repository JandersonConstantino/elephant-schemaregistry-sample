FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Elephant.Kafka.SchemaRegistry.Sample/Elephant.Kafka.SchemaRegistry.Sample.csproj", "Elephant.Kafka.SchemaRegistry.Sample/"]
RUN dotnet restore "Elephant.Kafka.SchemaRegistry.Sample/Elephant.Kafka.SchemaRegistry.Sample.csproj"
COPY . .
WORKDIR "/src/Elephant.Kafka.SchemaRegistry.Sample"
RUN dotnet build "./Elephant.Kafka.SchemaRegistry.Sample.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Elephant.Kafka.SchemaRegistry.Sample.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Elephant.Kafka.SchemaRegistry.Sample.dll"]
