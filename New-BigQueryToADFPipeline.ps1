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
    [string]$BQProjectID,

    [Parameter(Mandatory = $false, HelpMessage = "Log analytics workspace ID.")]
    [string]$LogAnalyticsWorkspaceName
)

# Ensure Az PowerShell module is installed
if (-not $(Get-Module -Name Az* -ListAvailable -ErrorAction SilentlyContinue)) {
    Write-Host "Az module not found. Installing..." -ForegroundColor Yellow
    Install-Module -Name Az -AllowClobber -Force -SkipPublisherCheck -Scope CurrentUser -confirm:$false
}

if (-not (Get-Module -Name newtonsoft.json -ListAvailable -ErrorAction SilentlyContinue)) {
    Write-Host "Newtonsoft.Json module not found. Installing..." -ForegroundColor Yellow
    Install-Module -Name Newtonsoft.Json -AllowClobber -Force -Scope CurrentUser -confirm:$false -SkipPublisherCheck
    Import-Module -Name Newtonsoft.Json -ErrorAction Stop
}

# Ensure there is an active Az PowerShell login/session and credentials are not expired
try {
    $azContext = Get-AzContext
    if (-not $azContext -or -not $azContext.Account -or -not $azContext.Account.Id) {
        # Try interactive login first
        Connect-AzAccount -ErrorAction Stop
        $azContext = Get-AzContext
    }
    else {
        # Check for expired credentials or missing token cache
        try {
            # Try to get an access token for validation
            $null = Get-AzAccessToken -ErrorAction Stop
        }
        catch {
            Write-Host "Interactive login failed or not possible, falling back to device authentication..." -ForegroundColor Yellow
            Connect-AzAccount -UseDeviceAuthentication -ErrorAction Stop
            $azContext = Get-AzContext
        }
    }
    if (-not $azContext -or -not $azContext.Account -or -not $azContext.Account.Id) {
        Write-Host "Failed to establish an active Az PowerShell session." -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "Failed to establish an active Az PowerShell session: $_" -ForegroundColor Red
    exit 1
}

# Check if resource group exists, if not create it
Write-Host "Checking if resource group '$ResourceGroupName' exists..." -ForegroundColor Yellow
if (-not $(Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue)) {
    Write-Host "Resource group '$ResourceGroupName' does not exist. Creating it now..." -ForegroundColor Yellow
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
if ($LogAnalyticsWorkspaceName) {
    Write-Host "Configuring Diagnostics settings on Data Factory - Log Analytics workspace: $LogAnalyticsWorkspaceName" -ForegroundColor Green
    $workspace = Get-AzOperationalInsightsWorkspace -ResourceGroupName $ResourceGroupName -Name $LogAnalyticsWorkspaceName -ErrorAction SilentlyContinue
    if (-not $workspace) {
        Write-Host "Log Analytics workspace '$LogAnalyticsWorkspaceName' does not exist, creating ..." -ForegroundColor Yellow
        $workspace = New-AzOperationalInsightsWorkspace `
            -ResourceGroupName $ResourceGroupName `
            -Name $LogAnalyticsWorkspaceName `
            -Location $AzureRegion `
            -Sku "PerGB2018"
    }
    else {
        Write-Host "Log Analytics workspace '$LogAnalyticsWorkspaceName' already exists." -ForegroundColor Green
    }

    # Configure diagnostics settings
    $diagName = "$DataFactoryName-law"
    $existingDiag = Get-AzDiagnosticSetting -ResourceId $adf.DataFactoryId -ErrorAction SilentlyContinue | Where-Object { $_.Name -eq $diagName }
    if (-not $existingDiag) {
        Write-Host "Creating diagnostic settings '$diagName' for Data Factory..." -ForegroundColor Yellow
        # Create diagnostic settings for logs and metrics using Az module object constructors
        $logCategories = @("ActivityRuns", "PipelineRuns", "TriggerRuns")
        $log = @()
        foreach ($cat in $logCategories) {
            $log += New-AzDiagnosticSettingLogSettingsObject -Category $cat -Enabled $true 
        }
        $metric = @()
        $metric += New-AzDiagnosticSettingMetricSettingsObject -Category "AllMetrics" -Enabled $true

        New-AzDiagnosticSetting `
            -ResourceId $adf.DataFactoryId `
            -WorkspaceId $workspace.ResourceId `
            -Name $diagName `
            -Log $log `
            -Metric $metric
    }
    else {
        Write-Host "Diagnostic settings '$diagName' already exist for Data Factory." -ForegroundColor Green
    }
}

# Add current public IP to allow list
try {
    $myIp = (Invoke-RestMethod -Uri "https://api.ipify.org?format=text" -UseBasicParsing).Trim()
    $myIp = (Invoke-RestMethod -Uri "https://api.ipify.org?format=text").Trim()
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

try {
    $tables = Import-Csv -Path $CSVFile -ErrorAction Stop
}
catch {
    Write-Host "Failed to import CSV file '$CSVFile'. The file may be malformed or unreadable. Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

if (-not $tables -or -not $tables.table_Id) {
    Write-Host "CSV file does not contain 'table_Id' column or is empty. Please ensure the CSV is formatted correctly." -ForegroundColor Red
    exit 1
}

# 3) CHECK PERMISSIONS on storage account before creating containers
function Test-StorageAccountPermission {
    param($StorageAccountName)
    try {
        $ctx = (Get-AzStorageAccount -Name $StorageAccountName -ResourceGroupName $ResourceGroupName -ErrorAction Stop).Context
        # Try to list containers as a permission check
        Get-AzStorageContainer -Context $ctx -ErrorAction Stop | Out-Null
        return $true
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

# Ensure a container named 'pipelinelogs' exists at the root of the storage account
$logsContainerName = "pipelinelogs"
try {
    $logsContainer = Get-AzStorageContainer -Name $logsContainerName -Context (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).Context -ErrorAction SilentlyContinue
    if (-not $logsContainer) {
        Write-Host "Creating container: $logsContainerName"
        $newLogsContainer = New-AzStorageContainer -Name $logsContainerName -Context (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).Context -ErrorAction Stop
        if ($newLogsContainer) {
            Write-Host "Container '$logsContainerName' created successfully." -ForegroundColor Green
        } else {
            Write-Host "Failed to create container '$logsContainerName'." -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "Container '$logsContainerName' already exists." -ForegroundColor Green
    }
} catch {
    Write-Host "An error occurred while ensuring the 'pipelinelogs' container exists: $_" -ForegroundColor Red
    exit 1
}

$tableList = @()
foreach ($t in $tables) {
    # Azure container names must be lowercase, 3-63 chars, alphanumeric or hyphen, start/end with letter/number
    $originalTableName = $t.table_Id
    $cname = $t.table_Id.ToLower() -replace '[^a-z0-9-]', '-' # replace invalid chars with hyphen
    $cname = $cname.Trim('-') # remove leading/trailing hyphens
    if ([string]::IsNullOrWhiteSpace($cname)) { $cname = "container000" }
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

    $tableList += [PSCustomObject]@{
        tableName    = $originalTableName
        containerName = $cname
        datasetName  = $BQDatasetID
        blobFileName = $cname                  # Only the base name is set here; file extension and timestamp are appended dynamically by the ADF pipeline, not in this script
        outputFormat = $OutputFormat
    }
}

#── 4. Pre-req variables ────────────────────────────────────────────────
# (assumes $rg, $dataFactory, $saName, $ctx already defined)
# Linked services already exist:
#   • GoogleBigQueryLinkedService
#   • AzureBlobStorageLinkedService
# 4) CREATE LINKED SERVICES

# setup the Azure Blob Storage linked service
function ConvertTo-AdfSafeJson {
    param (
        [Parameter(Mandatory)]
        [object]$InputObject,

        [ValidateSet("Indented", "None")]
        [string]$Formatting = "Indented"
    )

    if (-not ([AppDomain]::CurrentDomain.GetAssemblies() | Where-Object { $_.GetName().Name -eq 'Newtonsoft.Json' })) {
        Add-Type -AssemblyName 'Newtonsoft.Json'
    }

    # Step 2: Serialize with Newtonsoft and ignore reference loops
    $settings = New-Object Newtonsoft.Json.JsonSerializerSettings
    $settings.ReferenceLoopHandling = [Newtonsoft.Json.ReferenceLoopHandling]::Ignore
    $settings.Formatting = [Newtonsoft.Json.Formatting]::$Formatting

    # Step 2: Force all nested objects into .NET-safe representations (JObject handles this better than SerializeObject)
    $jObject = [Newtonsoft.Json.Linq.JObject]::FromObject($InputObject, [Newtonsoft.Json.JsonSerializer]::Create($settings))

    # Step 3: Serialize to JSON string
    $rawJson = $jObject.ToString()

    # Strip quotes from known ADF expressions
    # NOTE: This regex only matches simple ADF expressions starting with the listed keywords and may not cover all valid ADF expressions or edge cases (e.g., nested or complex expressions).
    # For complex scenarios, manual review of the generated JSON may be required to ensure correct handling of ADF expressions.
    <#$expressionKeywords = @(
        'dataset', 'pipeline', 'activity', 'item', 'trigger',
        'variables', 'concat', 'equals', 'if', 'and', 'or',
        'not', 'greater', 'less', 'addDays', 'utcNow', 'format', 'substring'
    ) -join '|' 
     #>

    #$regex = '"(@(?:' + $expressionKeywords + ')[^"]*?)"'
    #$safeJson = $rawJson -replace $regex, '$1'

    return $rawJson
}

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
    $blobStorageLs = [ordered]@{
        name       = "AzureBlobStorageLinkedService"
        properties = [ordered]@{
            type           = "AzureBlobStorage"
            typeProperties = [ordered]@{
                serviceEndpoint = $($blobEndpoint).ToString()
                authentication  = "ManagedIdentity"
                accountKind     = "StorageV2"
            }
        }
    }
}
else {
    Write-Host "Failed to retrieve blob service endpoint." -ForegroundColor Red
    exit 1
}

[string]$blobStorageLsJson = ConvertTo-AdfSafeJson -InputObject $blobStorageLs 
$blobLsTmp = [System.IO.Path]::GetTempFileName()
Set-Content -Path $blobLsTmp -Value $blobStorageLsJson -Encoding UTF8

# Deploy the linked service if it doesn't exist
$existingBlobLs = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -ErrorAction SilentlyContinue
$filteredBlob = $existingBlobLs | Where-Object { $_.Properties -match "Blob" }

if (-not $filteredBlob) {
    Write-Host "Creating new Azure Blob Storage linked service: $($blobStorageLs.name)" -ForegroundColor Green
    Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $blobStorageLs.name -DefinitionFile $blobLsTmp
    Write-Host "Azure Blob Storage linked service created successfully." -ForegroundColor Green
    # Get linked services again to ensure we have the latest
    Start-Sleep -Seconds 5 # Wait for the service to be created
    $existingBlobLs = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -ErrorAction SilentlyContinue
    $filteredBlob = $existingBlobLs | Where-Object { $_.Properties -match "Blob" }
}
if (-not $filteredBlob) {
    Write-Host "Failed to create or retrieve Azure Blob Storage linked service." -ForegroundColor Red
    exit 1
}

# Find the BigQuery linked service
$bigQueryLsName = $($existingBlobLs | Where-Object { $_.Properties -match "BigQuery" }).Name

if (-not $bigQueryLsName) {
    $i = 0
    While (-not $bigQueryLsName -and $i -lt 3) {
        Write-Host "No BigQuery linked service found." -ForegroundColor Red
        Write-Host "Please create one in Data Factory first.`n
        You can follow the instructions in the README to setup, the script will wait for you to complete" -ForegroundColor Yellow
        $matchBQName = Read-Host "Enter the name of the BigQuery linked service to use"
        $existingBlobLs = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -ErrorAction SilentlyContinue
        $bigQueryLsName = $($existingBlobLs | Where-Object { $_.Name -match $matchBQName }).Name
        if (-not $bigQueryLsName) {
            Write-Host "No BigQuery linked service found with the name '$matchBQName'. Please try again." -ForegroundColor Red
            $i++
        }
        else {
            Write-Host "Found BigQuery linked service: $bigQueryLsName" -ForegroundColor Green
            $i=3
        }

    }
    if (-not $bigQueryLsName) {
        Write-Host "No BigQuery linked service found after 3 attempts. Exiting." -ForegroundColor Red
        exit 1
    }
}
elseif ($bigQueryLsName.Count -gt 1) {
    Write-Host "Multiple BigQuery linked services found" -ForegroundColor Yellow
    $availableNames = $bigQueryLsName | ForEach-Object { $_.Name }
    Write-Output $availableNames
    $matchBQName = $null
    while (-not $availableNames -contains $matchBQName) {
        $matchBQName = Read-Host "Please specify the BigQuery linked service name to use from the list above"
        if (-not $availableNames -contains $matchBQName) {
            Write-Host "Invalid selection. Please enter a valid linked service name from the list." -ForegroundColor Red
        }
    }
    $bigQueryLsName = $matchBQName
}

# Find the Azure Blob Storage linked service
$blobStorageLsName = $($existingBlobLs | Where-Object { $_.Properties -match "Blob" }).Name

if (-not $blobStorageLsName) {
    $i = 0
    While (-not $blobStorageLsName -and $i -lt 3) {
        Write-Host "No Azure Blob Storage linked service found" -ForegroundColor Red
        Write-Host "Please create an Azure Blob Storage linked service in Data Factory first.`n
        You can follow the instructions in the README to setup, the script will wait for you to complete" -ForegroundColor Yellow
        $blobStorageLsName = Read-Host "Enter the name of the Azure Blob Storage linked service to use"
        # Find the Azure Blob Storage linked service
        $existingBlobLs = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -ErrorAction SilentlyContinue
        $blobStorageLsName = $existingBlobLs | Where-Object {$_.Name -match $blobStorageLsName }
        if (-not $blobStorageLsName) {
            Write-Host "No Azure Blob Storage linked service found with the name '$blobStorageLsName'. Please try again." -ForegroundColor Red
            $i++
        }
        else{
            $i=3
        }
    }
}
elseif ($blobStorageLsName.Count -gt 1) {
    Write-Host "Multiple Azure Blob Storage linked services found" -ForegroundColor Yellow
    $availableNames = $blobStorageLsName | ForEach-Object { $_.Name }
    Write-Output $availableNames
    $matchBlobName = $null
    while (-not $availableNames -contains $matchBlobName) {
        $matchBlobName = Read-Host "Please specify the Blob Storage linked service name to use from the list above"
        if (-not $availableNames -contains $matchBlobName) {
            Write-Host "Invalid selection. Please enter a valid linked service name from the list." -ForegroundColor Red
        }
    }
    $blobStorageLsName = $matchBlobName
}

Write-Host "Using BigQuery LS:     $bigQueryLsName"
Write-Host "Using BlobStorage LS:  $blobStorageLsName"

#── 4.1. BUILD & DEPLOY BIGQUERY SOURCE DATASET ─────────────────────────
$bqDs = [ordered]@{
    name       = "BigQueryDataset"
    properties = [ordered]@{
        type              = "GoogleBigQueryV2Object"
        linkedServiceName = [ordered]@{ referenceName = $bigQueryLsName; type = "LinkedServiceReference" }
        parameters        = [ordered]@{
            dataset   = [ordered]@{ type = "String" }
            tableName = [ordered]@{ type = "String" }
        }
        typeProperties    = [ordered]@{
            dataset = @{ type = "Expression"; value = "@dataset().dataset" }
            table   = @{ type = "Expression"; value = "@dataset().tableName" }
        }
    }
}

$bqDsJson = ConvertTo-AdfSafeJson -InputObject $bqDs
$bqDsTmp = [System.IO.Path]::GetTempFileName()
Set-Content -Path $bqDsTmp -Value $bqDsJson -Encoding UTF8

# Deploy BigQuery dataset
$existingBqDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $bqDs.name -ErrorAction SilentlyContinue

if ($existingBqDs) {
    Write-Host "BigQuery dataset '$($bqDs.name)' already exists, updating..." -ForegroundColor Green
    # Update the existing dataset with the new definition
    try {
        Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $bqDs.name -DefinitionFile $bqDsTmp -Confirm:$false -Force -Verbose
        Write-Host "BigQuery dataset updated successfully." -ForegroundColor Green
    }
    catch {
        Write-Host "Failed to update BigQuery dataset: $($bqDs.name)" -ForegroundColor Red
        Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}
else {
    try {
        Write-Host "Deploying BigQuery dataset: $($bqDs.name)" -ForegroundColor Green
        Set-AzDataFactoryV2Dataset `
            -ResourceGroupName $ResourceGroupName `
            -DataFactoryName    $DataFactoryName `
            -Name               $bqDs.name `
            -DefinitionFile     $bqDsTmp `
            -Verbose
    }
    catch {
        Write-Host "Failed to deploy BigQuery dataset: $($bqDs.name)" -ForegroundColor Red
        Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}

#── 4.2. BUILD & DEPLOY BLOB SINK DATASET (JSON) ────────────────────────
if ($OutputFormat) {
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
                    fileName  = "@dataset().blobFileName"
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

    $jsonDsJson = ConvertTo-AdfSafeJson -InputObject $jsonDs
    $jsonDsTmp = [System.IO.Path]::GetTempFileName()
    Set-Content -Path $jsonDsTmp -Value $jsonDsJson -Encoding UTF8

    $existingJsonDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $jsonDs.name -ErrorAction SilentlyContinue

    if ($existingJsonDs) {
        Write-Host "JSON sink dataset '$($jsonDs.name)' already exists, updating..." -ForegroundColor Green
        # Update the existing dataset with the new definition
        Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $jsonDs.name -DefinitionFile $jsonDsTmp -Confirm:$false -Force -Verbose
    }
    else {
        Write-Host "Deploying JSON sink dataset: $($jsonDs.name)" -ForegroundColor Green
        try {
            Set-AzDataFactoryV2Dataset `
                -ResourceGroupName $ResourceGroupName `
                -DataFactoryName    $DataFactoryName `
                -Name               $jsonDs.name `
                -DefinitionFile     $jsonDsTmp `
                -Confirm:$false `
                -Force `
                -Verbose
        }
        catch {
            Write-Host "Failed to deploy JSON sink dataset: $($jsonDs.name)" -ForegroundColor Red
            Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
            exit 1
        }
    }
}

#── 4.3. BUILD & DEPLOY BLOB SINK DATASET (PARQUET) ─────────────────────
if ($OutputFormat) {
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
                    fileName  = "@dataset().blobFileName"
                }
            }
            format            = [ordered]@{
                type = "ParquetFormat"
            }
        }
    }

    $parquetDsJson = ConvertTo-AdfSafeJson -InputObject $parquetDs 
    $parquetDsTmp = [System.IO.Path]::GetTempFileName()
    Set-Content -Path $parquetDsTmp -Value $parquetDsJson -Encoding UTF8

    $existingParquetDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $parquetDs.name -ErrorAction SilentlyContinue

    if ($existingParquetDs) {
        Write-Host "Parquet sink dataset '$($parquetDs.name)' already exists, updating..." -ForegroundColor Green
        # Update the existing dataset with the new definition
        Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $parquetDs.name -DefinitionFile $parquetDsTmp -Confirm:$false -Force -Verbose
    }
    else {
        try {
            Set-AzDataFactoryV2Dataset `
                -ResourceGroupName $ResourceGroupName `
                -DataFactoryName    $DataFactoryName `
                -Name               $parquetDs.name `
                -DefinitionFile     $parquetDsTmp `
                -Confirm:$false `
                -Force `
                -Verbose
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
    name       = "MainPipeline"
    properties = [ordered]@{ activities = @() }
}

Write-Host "Will create child pipelines for each table in the CSV file. Total count is $($tableList.Count)" -ForegroundColor Yellow

# Add log datasets for writing log to blob storage (only need to create once per Data Factory)
# LogInputDataset: Inline dataset for log content
# LogOutputDataset: Blob dataset for log file
$logInputDs = [ordered]@{
    name = "LogInputDataset"
    properties = [ordered]@{
        type = "Json"
        linkedServiceName = [ordered]@{ referenceName = $blobStorageLsName; type = "LinkedServiceReference" }
        parameters = [ordered]@{
            logContent = [ordered]@{ type = "String" }
        }
        typeProperties = [ordered]@{
            location = [ordered]@{
                type = "AzureBlobStorageLocation"
                container = "pipelinelogs"
                fileName = "*.json"
            }
        }
        structure = @(
            [ordered]@{
                name = "logContent"
                type = "String"
            }
        )
    }
}
$logOutputDs = [ordered]@{
    name = "LogOutputDataset"
    properties = [ordered]@{
        type = "Json"
        linkedServiceName = [ordered]@{ referenceName = $blobStorageLsName; type = "LinkedServiceReference" }
        parameters = [ordered]@{
            logFileName = [ordered]@{ type = "String" }
        }
        typeProperties = [ordered]@{
            location = [ordered]@{
                type = "AzureBlobStorageLocation"
                container = "pipelinelogs"
                fileName  = "@dataset().logFileName"
            }
        }
    }
}

# Convert log input dataset to JSON and save to temporary files
$logInputDsJson = ConvertTo-AdfSafeJson -InputObject $logInputDs
$logInputDsTmp = [System.IO.Path]::GetTempFileName()
Set-Content -Path $logInputDsTmp -Value $logInputDsJson -Encoding UTF8

# Convert log output dataset to JSON and save to temporary file
$logOutputDsJson = ConvertTo-AdfSafeJson -InputObject $logOutputDs
$logOutputDsTmp = [System.IO.Path]::GetTempFileName()
Set-Content -Path $logOutputDsTmp -Value $logOutputDsJson -Encoding UTF8

# Deploy log datasets if not already present
$existingLogInputDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $logInputDs.name -ErrorAction SilentlyContinue
if (-not $existingLogInputDs) {
    Write-Host "Deploying LogInputDataset: $($logInputDs.name)" -ForegroundColor Green
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $logInputDs.name -DefinitionFile $logInputDsTmp -Confirm:$false -Force
}
else {
    Write-Host "LogInputDataset already exists: $($logInputDs.name), updated" -ForegroundColor Yellow
    # Update the existing dataset with the new definition
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $logInputDs.name -DefinitionFile $logInputDsTmp -Confirm:$false -Force
}
$existingLogOutputDs = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $logOutputDs.name -ErrorAction SilentlyContinue
if (-not $existingLogOutputDs) {
    Write-Host "Deploying LogOutputDataset: $($logOutputDs.name)" -ForegroundColor Green
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $logOutputDs.name -DefinitionFile $logOutputDsTmp -Confirm:$false -Force
}
else {
    Write-Host "LogOutputDataset already exists: $($logOutputDs.name), updated" -ForegroundColor Yellow
    # Update the existing dataset with the new definition
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $logOutputDs.name -DefinitionFile $logOutputDsTmp -Confirm:$false -Force
}
foreach ($row in $tableList) {
    $childName = "Copy_$($row.tableName)"
    Write-Host "Processing table: $($row.tableName) with child pipeline name: $childName" -ForegroundColor Cyan
    $child = [ordered]@{
        name       = $childName
        properties = [ordered]@{
            parameters = [ordered]@{
                datasetName   = [ordered]@{ type = "String" }
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
                            # Debug: Set variable with source dataset and parameters
                            [ordered]@{
                                name = "Debug_Source_Parquet"
                                type = "SetVariable"
                                typeProperties = [ordered]@{
                                    variableName = "debugSource"
                                    value = "@concat('Source dataset: ', pipeline().parameters.datasetName, ', Table: ', pipeline().parameters.tableName)"
                                }
                            },
                            [ordered]@{
                                name           = "Copy_Parquet"
                                type           = "Copy"
                                dependsOn      = @([ordered]@{
                                    activity = "Debug_Source_Parquet"
                                    dependencyConditions = @("Succeeded")
                                })
                                inputs         = @([ordered]@{ 
                                    referenceName = "BigQueryDataset"; 
                                    type = "DatasetReference"; 
                                    parameters = [ordered]@{
                                        dataset = "@pipeline().parameters.datasetName"   # <-- Pass correct datasetName
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
                            },
                            # Debug: Set variable with copy output
                            [ordered]@{
                                name = "Debug_CopyOutput_Parquet"
                                type = "SetVariable"
                                dependsOn = @([ordered]@{
                                    activity = "Copy_Parquet"
                                    dependencyConditions = @("Succeeded")
                                })
                                typeProperties = [ordered]@{
                                    variableName = "debugCopyOutput"
                                    value = "@string(activity('Copy_Parquet').output)"
                                }
                            },
                            # Logging activity for Parquet
                            [ordered]@{
                                name = "Log_Parquet"
                                type = "AppendVariable"
                                dependsOn = @([ordered]@{
                                    activity = "Debug_CopyOutput_Parquet"
                                    dependencyConditions = @("Succeeded")
                                })
                                typeProperties = [ordered]@{
                                    variableName = "logOutput"
                                    value = "@concat('{""tableName"":""',pipeline().parameters.tableName,'""','"", ""containerName"":""',pipeline().parameters.containerName,'""','"", ""blobFileName"":""',pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.parquet','""','"", ""outputFormat"":""',pipeline().parameters.outputFormat,'""','"", ""dataRead"":',activity('Copy_Parquet').output.dataRead,', ""copyOutput"":',string(activity('Copy_Parquet').output),'}')"
                                }
                            },
                            # Write log to pipelinelogs container at root
                            [ordered]@{
                                name = "WriteLog_Parquet"
                                type = "Copy"
                                dependsOn = @([ordered]@{
                                    activity = "Log_Parquet"
                                    dependencyConditions = @("Succeeded")
                                })
                                inputs = @(
                                    [ordered]@{
                                        referenceName = "LogInputDataset"
                                        type = "DatasetReference"
                                        parameters = [ordered]@{
                                            logContent = "@string(variables('logOutput'))"
                                        }
                                    }
                                )
                                outputs = @(
                                    [ordered]@{
                                        referenceName = "LogOutputDataset"
                                        type = "DatasetReference"
                                        parameters = [ordered]@{
                                            logFileName = "@concat('log_',pipeline().parameters.tableName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.json')"
                                        }
                                    }
                                )
                                typeProperties = [ordered]@{
                                    source = [ordered]@{
                                        type = "JsonSource"
                                    }
                                    sink = [ordered]@{
                                        type = "JsonSink"
                                        storeSettings = [ordered]@{
                                            type = "AzureBlobStorageWriteSettings"
                                        }
                                        formatSettings = [ordered]@{
                                            type = "JsonWriteSettings"
                                        }
                                    }
                                }
                            }
                        )
                        ifFalseActivities = @(
                            # Debug: Set variable with source dataset and parameters
                            [ordered]@{
                                name = "Debug_Source_JSON"
                                type = "SetVariable"
                                typeProperties = [ordered]@{
                                    variableName = "debugSource"
                                    value = "@concat('Source dataset: ', pipeline().parameters.datasetName, ', Table: ', pipeline().parameters.tableName)"
                                }
                            },
                            [ordered]@{
                                name           = "Copy_JSON"
                                type           = "Copy"
                                dependsOn      = @([ordered]@{
                                    activity = "Debug_Source_JSON"
                                    dependencyConditions = @("Succeeded")
                                })
                                inputs         = @([ordered]@{ 
                                    referenceName = "BigQueryDataset"; 
                                    type = "DatasetReference"; 
                                    parameters = [ordered]@{
                                        dataset = "@pipeline().parameters.datasetName"   # <-- Pass correct datasetName
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
                            },
                            # Debug: Set variable with copy output
                            [ordered]@{
                                name = "Debug_CopyOutput_JSON"
                                type = "SetVariable"
                                dependsOn = @([ordered]@{
                                    activity = "Copy_JSON"
                                    dependencyConditions = @("Succeeded")
                                })
                                typeProperties = [ordered]@{
                                    variableName = "debugCopyOutput"
                                    value = "@string(activity('Copy_JSON').output)"
                                }
                            },
                            # Logging activity for JSON
                            [ordered]@{
                                name = "Log_JSON"
                                type = "AppendVariable"
                                dependsOn = @([ordered]@{
                                    activity = "Debug_CopyOutput_JSON"
                                    dependencyConditions = @("Succeeded")
                                })
                                typeProperties = [ordered]@{
                                    variableName = "logOutput"
                                    value = "@concat('{""tableName"":""',pipeline().parameters.tableName,'""','"", ""containerName"":""',pipeline().parameters.containerName,'""','"", ""blobFileName"":""',pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.json','""','"", ""outputFormat"":""',pipeline().parameters.outputFormat,'""','"", ""dataRead"":',activity('Copy_JSON').output.dataRead,', ""copyOutput"":',string(activity('Copy_JSON').output),'}')"
                                }
                            },
                            # Write log to pipelinelogs container at root
                            [ordered]@{
                                name = "WriteLog_JSON"
                                type = "Copy"
                                dependsOn = @([ordered]@{
                                    activity = "Log_JSON"
                                    dependencyConditions = @("Succeeded")
                                })
                                inputs = @(
                                    [ordered]@{
                                        referenceName = "LogInputDataset"
                                        type = "DatasetReference"
                                        parameters = [ordered]@{
                                            logContent = "@string(variables('logOutput'))"
                                        }
                                    }
                                )
                                outputs = @(
                                    [ordered]@{
                                        referenceName = "LogOutputDataset"
                                        type = "DatasetReference"
                                        parameters = [ordered]@{
                                            logFileName = "@concat('log_',pipeline().parameters.tableName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.json')"
                                        }
                                    }
                                )
                                typeProperties = [ordered]@{
                                    source = [ordered]@{
                                        type = "JsonSource"
                                    }
                                    sink = [ordered]@{
                                        type = "JsonSink"
                                        storeSettings = [ordered]@{
                                            type = "AzureBlobStorageWriteSettings"
                                        }
                                        formatSettings = [ordered]@{
                                            type = "JsonWriteSettings"
                                        }
                                    }
                                }
                            }
                        )
                    }
                }
            )
            # Add variables for debugging and logging
            variables = [ordered]@{
                logOutput = [ordered]@{
                    type = "Array"
                    defaultValue = @()
                }
                debugSource = [ordered]@{
                    type = "String"
                    defaultValue = ""
                }
                debugCopyOutput = [ordered]@{
                    type = "String"
                    defaultValue = ""
                }
            }
        }
    }

    $childObj = ConvertTo-AdfSafeJson -InputObject $child 
    $childTmp = [System.IO.Path]::GetTempFileName()
    Set-Content -Path $childTmp -Value $childObj -Encoding UTF8

    # Check if child pipeline exists
    $existingChild = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -ErrorAction SilentlyContinue

    if ($existingChild) {
        Write-Host "Updating existing pipeline: $childName" -ForegroundColor Yellow
        $updateSucceeded = $false
        try {
            Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -DefinitionFile $childTmp -Confirm:$false -Force
            $updateSucceeded = $true
        } catch {
            Write-Host "Error updating pipeline $childName : $($_.Exception.Message). Retrying in 3 seconds..." -ForegroundColor Red
            Start-Sleep -Seconds 3
            try {
                Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -DefinitionFile $childTmp -Confirm:$false -Force
                $updateSucceeded = $true
            } catch {
                Write-Host "Failed to update pipeline $childName after retry: $($_.Exception.Message)" -ForegroundColor Red
                exit 1
            }
        }
    }
    else {
        Write-Host "Creating new pipeline: $childName" -ForegroundColor Green
        $createSucceeded = $false
        try {
            Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName `
                -DataFactoryName $DataFactoryName `
                -Name $childName `
                -DefinitionFile $childTmp `
                -Verbose
            $createSucceeded = $true
        } catch {
            Write-Host "Error creating pipeline $childName : $($_.Exception.Message). Retrying in 3 seconds..." -ForegroundColor Red
            Start-Sleep -Seconds 3
            try {
                Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName `
                    -DataFactoryName $DataFactoryName `
                    -Name $childName `
                    -DefinitionFile $childTmp `
                    -Verbose
                $createSucceeded = $true
            } catch {
                Write-Host "Failed to create pipeline $childName after retry: $($_.Exception.Message)" -ForegroundColor Red
                exit 1
            }
        }
    }

    # add child to parent
    $parent.properties.activities += [ordered]@{
        name           = "Run_$childName"
        type           = "ExecutePipeline"
        typeProperties = [ordered]@{
            pipeline   = [ordered]@{ referenceName = $childName; type = "PipelineReference" }
            parameters = [ordered]@{
                datasetName   = $row.datasetName
                tableName     = $row.tableName
                containerName = $row.containerName  # Use containerName as intended
                blobFileName  = $row.blobFileName
                outputFormat  = $row.outputFormat
            }
        }
    }
}
# deploy parent
$parentObj = ConvertTo-AdfSafeJson -InputObject $parent
$parentTmp = [System.IO.Path]::GetTempFileName()
Set-Content -Path $parentTmp -Value $parentObj -Encoding UTF8

Write-Host "Deploying main pipeline: $($parent.name)" -ForegroundColor Green
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName `
    -DataFactoryName $DataFactoryName `
    -Name $parent.name `
    -DefinitionFile $parentTmp `
    -Verbose `
    -Confirm:$false `
    -Force
