FROM microsoft/dotnet:2.1-aspnetcore-runtime AS base
WORKDIR /app
EXPOSE 80

FROM microsoft/dotnet:2.1-sdk AS build
WORKDIR /src
COPY Swift.Management/Swift.Management.csproj Swift.Management/
COPY Swift.Core/Swift.Core.csproj Swift.Core/
RUN dotnet restore Swift.Management/Swift.Management.csproj
COPY . .
WORKDIR /src/Swift.Management
RUN dotnet build Swift.Management.csproj -c Release -o /app

FROM build AS publish
RUN dotnet publish Swift.Management.csproj -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "Swift.Management.dll"]
