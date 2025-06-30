param(
    [Parameter(Mandatory = $true, HelpMessage = "Azure Resource Group name.")]
    [string]$ResourceGroupName,

    [Parameter(Mandatory = $true, HelpMessage = "Azure Data Factory name.")]
    [string]$DataFactoryName,

    [Parameter(HelpMessage = "Delete all pipelines.")]
    [switch]$DeleteAllPipelines,

    [Parameter(HelpMessage = "Delete all datasets.")]
    [switch]$DeleteAllDatasets
)

# ... (connection code unchanged)



if ($DeleteAllPipelines) {
    # Get all pipelines (published)
    Write-Host "Retrieving pipelines from Data Factory '$DataFactoryName' in Resource Group '$ResourceGroupName'..." -ForegroundColor Cyan
    $pipelines = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Verbose

    # Delete "MainPipeline" first if it exists
    $mainPipeline = $pipelines | Where-Object { $_.Name -eq "MainPipeline" }
    if ($mainPipeline) {
        Write-Host "Deleting pipeline MainPipeline (DeleteAllPipelines switch set)."
        Remove-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name "MainPipeline" -Force
        # Remove MainPipeline from the list to avoid double deletion
        $pipelines = $pipelines | Where-Object { $_.Name -ne "MainPipeline" }
    }

    # Delete remaining pipelines
    foreach ($pipeline in $pipelines) {
        Write-Host "Deleting pipeline $($pipeline.Name) (DeleteAllPipelines switch set)."
        Remove-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $pipeline.Name -Force
    }
    return
}

if ($DeleteAllDatasets) {
    # Get all datasets
    Write-Host "Retrieving datasets from Data Factory '$DataFactoryName' in Resource Group '$ResourceGroupName'..." -ForegroundColor Cyan
    $datasets = Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName
    foreach ($dataset in $datasets) {
        Write-Host "Deleting dataset $($dataset.Name) (DeleteAllDatasets switch set)."
        Remove-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $dataset.Name -Force
    }
    return
}

if (-not $DeleteAllPipelines -and -not $DeleteAllDatasets) {
    Write-Host "No deletion switches set. Exiting without changes." -ForegroundColor Yellow
    return
}
# ... (existing dataset deletion logic)
