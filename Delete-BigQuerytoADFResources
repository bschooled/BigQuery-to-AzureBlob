param(
    [Parameter(Mandatory=$true, HelpMessage="Azure Resource Group name.")]
    [string]$rg
)

Write-Host "Are you sure you want to delete all resources in resource group '$rg'? This action cannot be undone." -ForegroundColor Yellow
$confirmation = Read-Host "Type 'yes' to confirm"
if ($confirmation -eq 'yes') {
    # Delete resources
    Write-Host "Deleting resources in resource group '$rg'..." -ForegroundColor Green
    try {
        Remove-AzResourceGroup -Name $rg -Force -ErrorAction Stop
        Write-Host "All resources in resource group '$rg' have been successfully deleted." -ForegroundColor Green
    } catch {
        Write-Host "An error occurred while deleting resources: $_" -ForegroundColor Red
    }
} else {
    Write-Host "Operation cancelled." -ForegroundColor Red
}