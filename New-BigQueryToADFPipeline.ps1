# Prereqs:
#  - Az PowerShell module installed & logged in (Connect-AzAccount)
# -- CONFIG -- #
param(
    [Parameter(Mandatory = $true, HelpMessage = "Azure Resource Group name.")]
    [string]$ResourceGroupName,

    [Parameter(Mandatory = $true, HelpMessage = "Globally unique Azure Storage Account name.")]
    [string]$StorageAccountName,

    [Parameter(Mandatory = $true, HelpMessage = "Azure region/location (e.g., eastus).")]
    [string]$AzureRegion,

    [Parameter(Mandatory = $true, HelpMessage = "Azure Data Factory name.")]
    [string]$DataFactoryName,

    [Parameter(Mandatory = $true, HelpMessage = "Path to CSV file with BigQuery table metadata.")]
    [string]$CSVFile,

    [Parameter(Mandatory = $true, HelpMessage = "Output format: 'json' or 'parquet'.")]
    [ValidateSet("json", "parquet")]
    [string]$OutputFormat,

    [Parameter(Mandatory = $true, HelpMessage = "Dataset ID for the BigQuery tables.")]
    [string]$BQDatasetID,

    [Parameter(Mandatory = $true, HelpMessage = "BigQuery project ID.")]
    [string]$BQProjectID
)

# Ensure Az PowerShell module is installed
if (-not $(Get-Module -Name Az* -ListAvailable -ErrorAction SilentlyContinue)) {
    Write-Host "Az module not found. Installing..." -ForegroundColor Yellow
    Install-Module -Name Az -AllowClobber -Force -SkipPublisherCheck -Scope CurrentUser
}

# Ensure Azure CLI is authenticated and import context into Az PowerShell if possible
if (-not (az account show)) {
    Write-Host "You are not logged in to Azure CLI. Please log in using 'az login'." -ForegroundColor Red
    az login --use-device-code
    if (-not (az account show)) {
        Write-Host "Failed to authenticate with Azure CLI. Please check your Azure credentials." -ForegroundColor Red
        exit 1
    }
}

# Try to import Azure CLI context into Az PowerShell (requires Az.Accounts >= 2.2.0)
if (-not (Get-AzContext)) {
    Write-Host "Missing Az context, attempting to import Azure CLI context into Az PowerShell..." -ForegroundColor Yellow
    try {
        $token = az account get-access-token --resource https://management.azure.com/ --query accessToken -o tsv
        $accountId = az account show --query user.name -o tsv
        $tenantId = az account show --query tenantId -o tsv
        Connect-AzAccount -AccessToken $token -AccountId $accountId -TenantId $tenantId -ErrorAction Stop
    }
    catch {
        Write-Host "Failed to import Azure CLI context or establish Azure context in PowerShell." -ForegroundColor Red
        exit 1
    }
}

# Check if resource group exists, if not create it
Write-Host "Checking if resource group '$ResourceGroupName' exists..." -ForegroundColor Yellow
if (-not $(Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue)) {
    Write-Host "Resource group '$ResourceGroupName' does not exist. Please create it first." -ForegroundColor Red
    # 1) CREATE RESOURCE GROUP
    New-AzResourceGroup -Name $ResourceGroupName -Location $AzureRegion -Force
}
else {
    Write-Host "Resource group '$ResourceGroupName' already exists." -ForegroundColor Green
}


Write-Host "Checking if Data Factory '$DataFactoryName' exists..." -ForegroundColor Yellow
if (-not $(Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -ErrorAction SilentlyContinue)) {
    Write-Host "Data Factory '$DataFactoryName' does not exist. Creating it now..." -ForegroundColor Yellow

    try {
        New-AzDataFactoryV2 `
            -ResourceGroupName $ResourceGroupName `
            -Name $DataFactoryName `
            -Location $AzureRegion `
            -IdentityType SystemAssigned
    }
    catch {
        Write-Host "An error occurred while creating the Data Factory: $_" -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "Data Factory '$DataFactoryName' already exists, ensuring managed identity is set up..." -ForegroundColor Yellow
    $adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName
    if ($adf.Identity.Type -ne "SystemAssigned") {
        Write-Host "Enabling SystemAssigned managed identity for Data Factory..." -ForegroundColor Yellow
        Update-AzDataFactoryV2 `
            -ResourceGroupName $ResourceGroupName `
            -Name $DataFactoryName `
            -IdentityType SystemAssigned
    }
    else {
        Write-Host "SystemAssigned managed identity is already enabled for Data Factory." -ForegroundColor Green
    }
}

# Add current public IP to allow list
try {
    $myIp = (Invoke-RestMethod -Uri "https://api.ipify.org?format=text" -UseBasicParsing).Trim()
    Write-Host "Detected public IP: $myIp" -ForegroundColor Green
}
catch {
    Write-Host "Failed to retrieve or add your public IP address. Please check your internet connection." -ForegroundColor Red
}

# Example: Add your allowed virtual network resource IDs here
$allowedVNetResourceIds = @(
    # "/subscriptions/<subId>/resourceGroups/<rg>/providers/Microsoft.Network/virtualNetworks/<vnet>/subnets/<subnet>"
)

# Example: Add your allowed IPs or ranges here
$allowedIpRanges = @()
if ($myIp) {
    $allowedIpRanges += $myIp
}

# Get Data Factory resource ID
$dataFactoryResourceId = (Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName).DataFactoryId

# Check if storage account exists, if not create it
Write-Host "Checking if storage account '$StorageAccountName' exists..." -ForegroundColor Yellow
if (-not $(Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName -ErrorAction SilentlyContinue)) {
    Write-Host "Storage account '$StorageAccountName' does not exist. Creating it now..." -ForegroundColor Yellow

    try {
        New-AzStorageAccount `
            -ResourceGroupName $ResourceGroupName `
            -Name $StorageAccountName `
            -Location $AzureRegion `
            -SkuName Standard_LRS `
            -Kind StorageV2 `
            -EnableHttpsTrafficOnly $true

        # Set network rules: Deny by default, allow AzureServices, allow select VNets, IPs, and Data Factory resource ID
        Write-Host "Configuring storage account network rules: Deny by default, allow AzureServices, select VNets, IPs, and Data Factory..." -ForegroundColor Yellow

        Update-AzStorageAccountNetworkRuleSet `
            -ResourceGroupName $ResourceGroupName `
            -Name $StorageAccountName `
            -DefaultAction Deny `
            -Bypass AzureServices

        foreach ($vnetId in $allowedVNetResourceIds) {
            Add-AzStorageAccountNetworkRule `
                -ResourceGroupName $ResourceGroupName `
                -Name $StorageAccountName `
                -VirtualNetworkResourceId $vnetId
        }

        foreach ($ip in $allowedIpRanges) {
            Add-AzStorageAccountNetworkRule `
                -ResourceGroupName $ResourceGroupName `
                -Name $StorageAccountName `
                -IPAddressOrRange $ip
        }

        # Add Data Factory resource ID to allow list
        if ($dataFactoryResourceId) {
            Add-AzStorageAccountNetworkRule `
                -ResourceGroupName $ResourceGroupName `
                -Name $StorageAccountName `
                -ResourceId $dataFactoryResourceId
            Write-Host "Added Data Factory resource ID '$dataFactoryResourceId' to the storage account network rules." -ForegroundColor Green
        }
        else {
            Write-Host "Could not determine Data Factory resource ID. Please check Data Factory existence." -ForegroundColor Red
            exit 1
        }

    }
    catch {
        Write-Host "An error occurred while creating the storage account: $_" -ForegroundColor Red
        exit 1
    }

}
else {
    Write-Host "Storage account '$StorageAccountName' already exists." -ForegroundColor Green
    try {
        # Get current network rule set
        $networkRules = Get-AzStorageAccountNetworkRuleSet -ResourceGroupName $ResourceGroupName -Name $StorageAccountName

        $bypassSet = $networkRules.Bypass -split ',' | ForEach-Object { $_.Trim() }
        $hasAzureServicesBypass = $bypassSet -contains "AzureServices"

        $hasMyIp = $false
        if ($networkRules.IpRules) {
            $hasMyIp = $networkRules.IpRules | Where-Object { $_.IPAddressOrRange -eq $myIp }
        }

        # Check if Data Factory resource ID is already allowed
        $hasDataFactoryResource = $false
        if ($networkRules.ResourceAccessRules) {
            $hasDataFactoryResource = $networkRules.ResourceAccessRules | Where-Object { $_.ResourceId -eq $dataFactoryResourceId }
        }

        # Set default action to Deny and add AzureServices bypass if not already set
        if ($networkRules.DefaultAction -ne "Deny" -or -not $hasAzureServicesBypass) {
            Write-Host "Updating storage account network rules to deny all networks except trusted Azure services..." -ForegroundColor Yellow
            Update-AzStorageAccountNetworkRuleSet `
                -ResourceGroupName $ResourceGroupName `
                -Name $StorageAccountName `
                -DefaultAction Deny `
                -Bypass AzureServices
        }
        else {
            Write-Host "Storage account network rules already set to Deny with AzureServices bypass." -ForegroundColor Green
        }

        # Add allowed VNets if not already present
        foreach ($vnetId in $allowedVNetResourceIds) {
            if (-not ($networkRules.VirtualNetworkRules | Where-Object { $_.VirtualNetworkResourceId -eq $vnetId })) {
                Write-Host "Adding VNet rule: $vnetId" -ForegroundColor Yellow
                Add-AzStorageAccountNetworkRule `
                    -ResourceGroupName $ResourceGroupName `
                    -Name $StorageAccountName `
                    -VirtualNetworkResourceId $vnetId
            }
        }

        # Add allowed IPs if not already present
        foreach ($ip in $allowedIpRanges) {
            if (-not ($networkRules.IpRules | Where-Object { $_.IPAddressOrRange -eq $ip })) {
                Write-Host "Adding IP rule: $ip" -ForegroundColor Yellow
                Add-AzStorageAccountNetworkRule `
                    -ResourceGroupName $ResourceGroupName `
                    -Name $StorageAccountName `
                    -IPAddressOrRange $ip
            }
        }

        # Add Data Factory resource ID if not already present
        if ($dataFactoryResourceId -and -not $hasDataFactoryResource) {
            Add-AzStorageAccountNetworkRule `
                -ResourceGroupName $ResourceGroupName `
                -Name $StorageAccountName `
                -ResourceId $dataFactoryResourceId
            Write-Host "Added Data Factory resource ID '$dataFactoryResourceId' to the storage account network rules." -ForegroundColor Green
        }
        elseif ($hasDataFactoryResource) {
            Write-Host "Data Factory resource ID '$dataFactoryResourceId' is already allowed in the storage account network rules." -ForegroundColor Green
        }
        else {
            Write-Host "Could not determine Data Factory resource ID. Please check Data Factory existence." -ForegroundColor Red
            exit 1
        }

        # Add current public IP to allow list if not already present
        if (-not $hasMyIp) {
            Write-Host "Adding your public IP '$myIp' to the storage account network rules..." -ForegroundColor Yellow
            Add-AzStorageAccountNetworkRule `
                -ResourceGroupName $ResourceGroupName `
                -Name $StorageAccountName `
                -IPAddressOrRange $myIp
        }
        else {
            Write-Host "Your public IP '$myIp' is already allowed in the storage account network rules." -ForegroundColor Green
        }
    }
    catch {
        Write-Host "An error occurred while updating the storage account network rules: $_" -ForegroundColor Red
        exit 1
    }
}


if (-not $(Test-Path -Path $CSVFile)) {
    Write-Host "CSV file '$CSVFile' does not exist. Please provide a valid path." -ForegroundColor Red
    Write-Host "If running from Cloud Shell, click the 'Upload/Download files' icon to upload your CSV file.`n" -ForegroundColor Yellow
    $CSVFile = Read-Host "Enter the path to your BigQuery tables CSV file"
}

$tables = Import-Csv -Path $CSVFile
if (-not $tables.table_Id) {
    Write-Host "CSV file does not contain 'table_Id' column. Please ensure the CSV is formatted correctly." -ForegroundColor Red
    exit 1
}

# 3) CHECK PERMISSIONS on storage account before creating containers
function Test-StorageAccountPermission {
    param($StorageAccountName)
    try {
        # Attempt to list containers using az CLI as a permission check
        az storage container list --account-name $StorageAccountName --auth-mode login --only-show-errors 2>$null
        if ($LASTEXITCODE -eq 0) {
            return $true
        }
        else {
            return $false
        }
    }
    catch {
        return $false
    }
}

$ctx = Test-StorageAccountPermission -StorageAccountName $StorageAccountName

if (-not $ctx) {
    Write-Host "You do not have permission to manage containers in storage account '$StorageAccountName'." -ForegroundColor Yellow

    # Check if user already has the Storage Blob Data Contributor role
    $scope = "/subscriptions/$((Get-AzContext).Subscription.Id)/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$StorageAccountName"
    $userObject = Get-AzADUser -UserPrincipalName (Get-AzContext).Account.Id -ErrorAction SilentlyContinue
    if (-not $userObject) {
        Write-Host "Could not find Azure AD user for $((Get-AzContext).Account.Id). Please ensure your account exists in Azure AD." -ForegroundColor Red
        exit 1
    }
    $userObjectId = $userObject.Id

    $existingRole = Get-AzRoleAssignment -ObjectId $userObjectId -Scope $scope -ErrorAction SilentlyContinue | Where-Object { $_.RoleDefinitionName -eq "Storage Blob Data Contributor" }
    if ($existingRole) {
        Write-Host "User already has 'Storage Blob Data Contributor' role. Waiting for permissions to propagate..." -ForegroundColor Yellow
        Start-Sleep -Seconds 15
        $ctx = Test-StorageAccountPermission -StorageAccountName $StorageAccountName
        if ($ctx) {
            Write-Host "Permission granted: Able to list containers in storage account '$StorageAccountName'." -ForegroundColor Green
        }
        else {
            Write-Host "Permission check still failed after confirming role assignment." -ForegroundColor Red
            Write-Host "Ensure your account or the Data Factory managed identity has 'Storage Blob Data Contributor' or higher role." -ForegroundColor Yellow
            exit 1
        }
    }
    else {
        Write-Host "Attempting to assign 'Storage Blob Data Contributor' role to user ObjectId: $userObjectId" -ForegroundColor Yellow
        Write-Host "Role assignment scope: $scope" -ForegroundColor Yellow
        try {
            New-AzRoleAssignment -ObjectId $userObjectId -RoleDefinitionName "Storage Blob Data Contributor" -Scope $scope -ErrorAction Stop
            Start-Sleep -Seconds 15 # Wait for role assignment to propagate
            $ctx = Test-StorageAccountPermission -StorageAccountName $StorageAccountName
            if ($ctx) {
                Write-Host "Permission granted: Able to list containers in storage account '$StorageAccountName'." -ForegroundColor Green
            }
            else {
                Write-Host "Role assignment was attempted but permission check still failed." -ForegroundColor Red
                Write-Host "Ensure your account or the Data Factory managed identity has 'Storage Blob Data Contributor' or higher role." -ForegroundColor Yellow
                exit 1
            }
        }
        catch {
            Write-Host "Failed to assign role or check permissions: $_" -ForegroundColor Red
            Write-Host "Ensure your account or the Data Factory managed identity has 'Storage Blob Data Contributor' or higher role." -ForegroundColor Yellow
            exit 1
        }
    }
}
else {
    Write-Host "Permission check passed: Able to list containers in storage account '$StorageAccountName'." -ForegroundColor Green
}

$tableList = foreach ($t in $tables) {
    # Azure container names must be lowercase, 3-63 chars, alphanumeric or hyphen, start/end with letter/number
    $cname = $t.table_Id.ToLower() -replace '[^a-z0-9-]', '-' # replace invalid chars with hyphen
    $cname = $cname.Trim('-') # remove leading/trailing hyphens
    if ($cname.Length -lt 3) { $cname = $cname.PadRight(3, '0') }
    if ($cname.Length -gt 63) { $cname = $cname.Substring(0, 63) }

    # Use Az PowerShell to check if container exists
    $containerExists = $false
    try {
        $container = Get-AzStorageContainer -Name $cname -Context (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).Context -ErrorAction SilentlyContinue
        if ($container) {
            $containerExists = $true
        }
    }
    catch {
        Write-Host "Failed to check existence of container '$cname' using Az PowerShell: $_" -ForegroundColor Red
        exit 1
    }

    if (-not $containerExists) {
        Write-Host "Creating container: $cname"
        try {
            $newContainer = New-AzStorageContainer -Name $cname -Context (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).Context -ErrorAction Stop
            if (-not $newContainer) {
                Write-Host "Failed to create container '$cname'. You may not have permission to create containers in this storage account." -ForegroundColor Red
                Write-Host "Ensure your logged in account has 'Storage Blob Data Contributor' or higher role on the storage account." -ForegroundColor Yellow
                exit 1
            }
            Write-Host "Container '$cname' created successfully." -ForegroundColor Green
        }
        catch {
            Write-Host "An error occurred while creating the storage container: $_" -ForegroundColor Red
            exit 1
        }
    }
    else {
        Write-Host "Container '$cname' already exists." -ForegroundColor Green
    }

    [PSCustomObject]@{
        tableName     = $cname
        bqDatasetId   = $BQDatasetID
        containerName = $cname
        blobFileName  = $cname                  # Only the base name is set here; file extension and timestamp are appended dynamically in the ADF pipeline
        outputFormat  = $OutputFormat
    }
}

#── 4. Pre-req variables ────────────────────────────────────────────────
# (assumes $rg, $dataFactory, $saName, $ctx already defined)
# Linked services already exist:
#   • GoogleBigQueryLinkedService
#   • AzureBlobStorageLinkedService
# 4) CREATE LINKED SERVICES

# setup the Azure Blob Storage linked service
$adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName
$adfIdentity = $adf.Identity.PrincipalId

# Assign Storage Blob Data Contributor role to Data Factory managed identity if not already assigned
$adfRoleAssignment = Get-AzRoleAssignment -ObjectId $adfIdentity `
    -RoleDefinitionName "Storage Blob Data Contributor" `
    -Scope "/subscriptions/$((Get-AzContext).Subscription.Id)/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$StorageAccountName" `
    -ErrorAction SilentlyContinue

if (-not $adfRoleAssignment) {
    Write-Host "Assigning 'Storage Blob Data Contributor' role to Data Factory managed identity..." -ForegroundColor Yellow
    New-AzRoleAssignment `
        -ObjectId $adfIdentity `
        -RoleDefinitionName "Storage Blob Data Contributor" `
        -Scope "/subscriptions/$((Get-AzContext).Subscription.Id)/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$StorageAccountName"
}
else {
    Write-Host "Data Factory managed identity already has 'Storage Blob Data Contributor' role on the storage account." -ForegroundColor Green
}

# get the blob service endpoint
$blobEndpoint = (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).PrimaryEndpoints.Blob

if ($blobEndpoint) {
    Write-Host "Blob service endpoint: $blobEndpoint" -ForegroundColor Green
    # Create the Azure Blob Storage linked service using system-assigned managed identity
    $blobStorageLs = @{
        name       = "AzureBlobStorageLinkedService"
        properties = @{
            type           = "AzureBlobStorage"
            typeProperties = @{
                serviceEndpoint = $blobEndpoint
                authentication  = "ManagedIdentity"
            }
        }
    }
}
else {
    Write-Host "Failed to retrieve blob service endpoint." -ForegroundColor Red
    exit 1
}

$blobStorageLs = $blobStorageLs | ConvertTo-Json -Depth 100 -Compress | ConvertFrom-Json

# Deploy the linked service if it doesn't exist
$existingBlobLs = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -ErrorAction SilentlyContinue
$filteredBlob = $existingBlobLs | Where-Object { $_.Properties -match "BigQuery" }

if (-not $filteredBlob) {
    Write-Host "Creating new Azure Blob Storage linked service: $($blobStorageLs.name)" -ForegroundColor Green
    Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $blobStorageLs.name -Definition $blobStorageLs
}

if (-not $filteredBlob) {
    Write-Host "Creating new Azure Blob Storage linked service: $($blobStorageLs.name)" -ForegroundColor Green
    Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $blobStorageLs.name -DefinitionFile $BlobLStmpFile
}

# Find the BigQuery linked 
$bigQueryLsName = $existingBlobLs | Where-Object { $_.Properties -match "BigQuery" }

if (-not $bigQueryLsName) {
    $i = 0
    While (-not $bigQueryLsName -and $i -lt 3) {
        Write-Host "No BigQuery linked service found." -ForegroundColor Red
        Write-Host "Please create one in Data Factory first.`n
        You can follow the instructions in the README to setup, the script will wait for you to complete" -ForegroundColor Yellow
        $bigQueryLsName = Read-Host "Enter the name of the BigQuery linked service to use"
        $bigQueryLsName = $existingBlobLs | Where-Object { $_.Properties -match "BigQuery" }
        $i++
    }
    if (-not $bigQueryLsName) {
        Write-Host "No BigQuery linked service found after 3 attempts. Exiting." -ForegroundColor Red
        exit 1
    }
}
elseif ($bigQueryLsName.Count -gt 1) {
    Write-Host "Multiple BigQuery linked services found" -ForegroundColor Yellow
    Write-Output $bigQueryLsName | ForEach-Object { $_.Name }
    $matchBQName = Read-Host "Please specify the BigQuery linked service name to use"
    $bigQueryLsName = $matchBQName
}

# Find the Azure Blob Storage linked service
$blobStorageLsName = $existingBlobLs | Where-Object { $_.Properties -match "Blob" }

if (-not $blobStorageLsName) {
    $i = 0
    While (-not $blobStorageLsName -and $i -lt 3) {
        Write-Host "No Azure Blob Storage linked service found" -ForegroundColor Red
        Write-Host "Please create an Azure Blob Storage linked service in Data Factory first.`n
        You can follow the instructions in the README to setup, the script will wait for you to complete" -ForegroundColor Yellow
        $blobStorageLsName = Read-Host "Enter the name of the Azure Blob Storage linked service to use"
        # Find the Azure Blob Storage linked service
        $blobStorageLsName = $existingBlobLs | Where-Object { $_.Properties -match "Blob" }
        $i++
    }
    if (-not $blobStorageLsName) {
        Write-Host "No Azure Blob Storage linked service found after 3 attempts. Exiting." -ForegroundColor Red
        exit 1
    }
}
elseif ($blobStorageLsName.Count -gt 1) {
    Write-Host "Multiple Azure Blob Storage linked services found" -ForegroundColor Yellow
    Write-Output $blobStorageLsName | ForEach-Object { $_.Name }
    $matchBlobName = Read-Host "Please specify the Blob Storage linked service name to use"
    $blobStorageLsName = $matchBlobName
}

Write-Host "Using BigQuery LS:     $bigQueryLsName"
Write-Host "Using BlobStorage LS:  $blobStorageLsName"


#── 4.1. BUILD & DEPLOY BIGQUERY SOURCE DATASET ─────────────────────────
$bqDs = [ordered]@{
    name       = "BigQueryDataset"
    properties = [ordered]@{
        type              = "GoogleBigQuery"
        linkedServiceName = [ordered]@{ referenceName = $bigQueryLsName; type = "LinkedServiceReference" }
        parameters        = [ordered]@{
            datasetName = [ordered]@{ type = "String" }
            tableName   = [ordered]@{ type = "String" }
        }
        typeProperties    = [ordered]@{
            tableName = "@concat(dataset().datasetName, '.', dataset().tableName)"
        }
    }
}

$bqDs = $bqDs | ConvertTo-Json -Depth 100 -Compress | ConvertFrom-Json

# Deploy BigQuery dataset
$existingBqDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $bqDs.name -ErrorAction SilentlyContinue

if ($existingBqDs) {
    Write-Host "BigQuery dataset '$($bqDs.name)' already exists. Skipping creation." -ForegroundColor Green
}
else {
    try {
        Write-Host "Deploying BigQuery dataset: $($bqDs.name)" -ForegroundColor Green
        Set-AzDataFactoryV2Dataset `
            -ResourceGroupName $ResourceGroupName `
            -DataFactoryName    $DataFactoryName `
            -Name               $bqDs.name `
            -Definition         $bqDs
    }
    catch {
        Write-Host "Failed to deploy BigQuery dataset: $($bqDs.name)" -ForegroundColor Red
        Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}

#── 4.2. BUILD & DEPLOY BLOB SINK DATASET (JSON) ────────────────────────
if ($OutputFormat -ieq "json") {
    $jsonDs = [ordered]@{
        name       = "BlobSink_JSON"
        properties = [ordered]@{
            type              = "Json"
            linkedServiceName = [ordered]@{ referenceName = $blobStorageLsName; type = "LinkedServiceReference" }
            parameters        = [ordered]@{
                containerName = [ordered]@{ type = "String" }
                blobFileName  = [ordered]@{ type = "String" }
            }
            typeProperties    = [ordered]@{
                location    = [ordered]@{
                    type      = "AzureBlobStorageLocation"
                    container = "@dataset().containerName"
                    blobPath  = "@dataset().blobFileName"
                }
                compression = [ordered]@{
                    type  = "Gzip"
                    level = "Optimal"
                }
            }
            format            = [ordered]@{
                type = "JsonFormat"
            }
        }
    }

    $jsonDs = $jsonDs | ConvertTo-Json -Depth 100 -Compress | ConvertFrom-Json
    $existingJsonDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $jsonDs.name -ErrorAction SilentlyContinue

    if ($existingJsonDs) {
        Write-Host "JSON sink dataset '$($jsonDs.name)' already exists. Skipping creation." -ForegroundColor Green
    }
    else {
        Write-Host "Deploying JSON sink dataset: $($jsonDs.name)" -ForegroundColor Green
        try {
            Set-AzDataFactoryV2Dataset `
                -ResourceGroupName $ResourceGroupName `
                -DataFactoryName    $DataFactoryName `
                -Name               $jsonDs.name `
                -Definition         $jsonDs.properties
        }
        catch {
            Write-Host "Failed to deploy JSON sink dataset: $($jsonDs.name)" -ForegroundColor Red
            Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
            exit 1
        }
    }
}

#── 4.3. BUILD & DEPLOY BLOB SINK DATASET (PARQUET) ─────────────────────
if ($OutputFormat -ieq "parquet") {
    $parquetDs = [ordered]@{
        name       = "BlobSink_Parquet"
        properties = [ordered]@{
            type              = "Parquet"
            linkedServiceName = [ordered]@{ referenceName = $blobStorageLsName; type = "LinkedServiceReference" }
            parameters        = [ordered]@{
                containerName = [ordered]@{ type = "String" }
                blobFileName  = [ordered]@{ type = "String" }
            }
            typeProperties    = [ordered]@{
                location = [ordered]@{
                    type      = "AzureBlobStorageLocation"
                    container = "@dataset().containerName"
                    blobPath  = "@dataset().blobFileName"
                }
            }
            format            = [ordered]@{
                type = "ParquetFormat"
            }
        }
    }

    $parquetDs = $parquetDs | ConvertTo-Json -Depth 100 -Compress | ConvertFrom-Json
    $existingParquetDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $parquetDs.name -ErrorAction SilentlyContinue

    if ($existingParquetDs) {
        Write-Host "Parquet sink dataset '$($parquetDs.name)' already exists. Skipping creation." -ForegroundColor Green
    }
    else {
        try {
            Set-AzDataFactoryV2Dataset `
                -ResourceGroupName $ResourceGroupName `
                -DataFactoryName    $DataFactoryName `
                -Name               $parquetDs.name `
                -Definition         $parquetDs
        }
        catch {
            Write-Host "Failed to deploy Parquet sink dataset: $($parquetDs.name)" -ForegroundColor Red
            Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
            exit 1
        }
    }
}

# 5) BUILD & DEPLOY ADF PIPES
$parent = [ordered]@{
    name       = "MasterPipeline"
    properties = [ordered]@{ activities = @() }
}

foreach ($row in $tableList) {
    $childName = "Copy_$($row.tableName)"
    $child = [ordered]@{
        name       = $childName
        properties = [ordered]@{
            parameters = [ordered]@{
                bqDatasetId   = [ordered]@{ type = "String" }
                tableName     = [ordered]@{ type = "String" }
                containerName = [ordered]@{ type = "String" }
                blobFileName  = [ordered]@{ type = "String" }
                outputFormat  = [ordered]@{ type = "String" }
            }
            activities = @(
                [ordered]@{
                    name           = "If_Parquet"
                    type           = "IfCondition"
                    typeProperties = [ordered]@{
                        expression        = [ordered]@{
                            type  = "Expression"
                            value = "@equals(toLower(pipeline().parameters.outputFormat),'parquet')"
                        }
                        ifTrueActivities  = @(
                            [ordered]@{
                                name           = "Copy_Parquet"
                                type           = "Copy"
                                inputs         = @([ordered]@{ referenceName = "BigQueryDataset"; type = "DatasetReference"; parameters = [ordered]@{
                                            datasetName = "@pipeline().parameters.bqDatasetId"
                                            tableName   = "@pipeline().parameters.tableName"
                                        }
                                    })
                                outputs        = @([ordered]@{ referenceName = "BlobSink_Parquet"; type = "DatasetReference"; parameters = [ordered]@{
                                            containerName = "@pipeline().parameters.containerName"
                                            blobFileName  = "@concat(pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.parquet')"
                                        }
                                    })
                                typeProperties = [ordered]@{
                                    source = [ordered]@{ type = "GoogleBigQuerySource" }
                                    sink   = [ordered]@{ type = "ParquetSink" }
                                }
                            }
                        )
                        ifFalseActivities = @(
                            [ordered]@{
                                name           = "Copy_JSON"
                                type           = "Copy"
                                inputs         = @([ordered]@{ referenceName = "BigQueryDataset"; type = "DatasetReference"; parameters = [ordered]@{
                                            datasetName = "@pipeline().parameters.bqDatasetId"
                                            tableName   = "@pipeline().parameters.tableName"
                                        }
                                    })
                                outputs        = @([ordered]@{ referenceName = "BlobSink_JSON"; type = "DatasetReference"; parameters = [ordered]@{
                                            containerName = "@pipeline().parameters.containerName"
                                            blobFileName  = "@concat(pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.json')"
                                        }
                                    })
                                typeProperties = [ordered]@{
                                    source = [ordered]@{ type = "GoogleBigQuerySource" }
                                    sink   = [ordered]@{
                                        type           = "JsonSink"
                                        storeSettings  = [ordered]@{ type = "AzureBlobStorageWriteSettings" }
                                        formatSettings = [ordered]@{
                                            type            = "JsonWriteSettings"
                                            fileCompression = [ordered]@{ type = "Gzip"; level = "Optimal" }
                                        }
                                    }
                                }
                            }
                        )
                    }
                }
            )
        }
    }

    $child = $child | ConvertTo-Json -Depth 100 -Compress | ConvertFrom-Json
    # Check if child pipeline exists
    $existingChild = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -ErrorAction SilentlyContinue

    if ($existingChild) {
        Write-Host "Updating existing pipeline: $childName" -ForegroundColor Yellow
        Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -Definition $child
    }
    else {
        Write-Host "Creating new pipeline: $childName" -ForegroundColor Green
        Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -Definition $child
    }

    # add child to parent
    $parent.properties.activities += [ordered]@{
        name           = "Run_$childName"
        type           = "ExecutePipeline"
        typeProperties = [ordered]@{
            pipeline   = [ordered]@{ referenceName = $childName; type = "PipelineReference" }
            parameters = [ordered]@{
                bqDatasetId   = $row.bqDatasetId
                tableName     = $row.tableName
                containerName = $row.containerName
                blobFileName  = $row.tableName
                outputFormat  = $row.outputFormat
            }
        }
    }
}
# deploy parent
$parent = $parent | ConvertTo-Json -Depth 100 -Compress | ConvertFrom-Json 
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $parent.name -Definition $parent
