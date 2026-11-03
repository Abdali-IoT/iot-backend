FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src

# Restore separately so source-only changes can reuse the dependency layer.
COPY API/API.csproj API/
RUN dotnet restore API/API.csproj

COPY . .
RUN dotnet publish API/API.csproj \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0-alpine AS runtime
WORKDIR /app
ENV ASPNETCORE_HTTP_PORTS=8081
EXPOSE 8081

COPY --from=build /app/publish .
USER $APP_UID
ENTRYPOINT ["dotnet", "API.dll"]