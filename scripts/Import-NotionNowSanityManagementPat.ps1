#requires -Version 7.0
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$profileKey = 'sanity-management'
$workspaceName = 'Sanity Management'
$secretRoot = Join-Path $env:LOCALAPPDATA 'SanityCloud\NotionNow\secrets'
$secretPath = Join-Path $secretRoot 'sanity-management-pat.dpapi'
$metadataPath = Join-Path $secretRoot 'sanity-management-pat.metadata.json'
$entropyLabel = 'SanityCloud|NotionNow|sanity-management|PAT|v1'

function Set-PrivateAcl {
    param([Parameter(Mandatory)][string]$Path)

    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $acl = New-Object System.Security.AccessControl.FileSecurity
    $acl.SetAccessRuleProtection($true, $false)
    $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        $identity,
        [System.Security.AccessControl.FileSystemRights]::FullControl,
        [System.Security.AccessControl.AccessControlType]::Allow
    )
    $acl.AddAccessRule($rule)
    [System.IO.File]::SetAccessControl($Path, $acl)
}

function Get-Sha256Hex {
    param([Parameter(Mandatory)][byte[]]$Bytes)

    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ([Convert]::ToHexString($sha.ComputeHash($Bytes))).ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

Clear-Host
Write-Host 'SanityCloud — NotionNow/Awkoy PAT Enrollment' -ForegroundColor Cyan
Write-Host 'Workspace: Sanity Management'
Write-Host ''
Write-Host 'Paste the PAT at the secure prompt and press Enter.' -ForegroundColor Yellow
Write-Host 'The token will not be displayed, echoed, or written to command history.'
Write-Host 'It will be validated with Notion and stored using Windows DPAPI (CurrentUser).'
Write-Host ''

$secureToken = $null
$bstr = [IntPtr]::Zero
$plainToken = $null
$plainBytes = $null

try {
    $secureToken = Read-Host 'Sanity Management Notion PAT' -AsSecureString
    if ($secureToken.Length -lt 16) {
        throw 'The supplied value is too short to be a valid Notion token.'
    }

    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureToken)
    $plainToken = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    if ([string]::IsNullOrWhiteSpace($plainToken)) {
        throw 'No token was supplied.'
    }

    Write-Host ''
    Write-Host 'Validating token with Notion...' -ForegroundColor DarkCyan
    $headers = @{
        Authorization = "Bearer $plainToken"
        'Notion-Version' = '2022-06-28'
        Accept = 'application/json'
    }
    $identity = Invoke-RestMethod -Method Get -Uri 'https://api.notion.com/v1/users/me' -Headers $headers -TimeoutSec 30

    New-Item -ItemType Directory -Force -Path $secretRoot | Out-Null

    $entropyBytes = [Text.Encoding]::UTF8.GetBytes($entropyLabel)
    $plainBytes = [Text.Encoding]::UTF8.GetBytes($plainToken)
    $cipherBytes = [Security.Cryptography.ProtectedData]::Protect(
        $plainBytes,
        $entropyBytes,
        [Security.Cryptography.DataProtectionScope]::CurrentUser
    )

    $secretTemp = "$secretPath.$PID.tmp"
    [IO.File]::WriteAllBytes($secretTemp, $cipherBytes)
    Set-PrivateAcl -Path $secretTemp
    Move-Item -LiteralPath $secretTemp -Destination $secretPath -Force
    Set-PrivateAcl -Path $secretPath

    $metadata = [ordered]@{
        schema_version = 1
        profile_key = $profileKey
        workspace_name = $workspaceName
        credential_type = 'notion_personal_access_token'
        consumer = 'NotionNow/Awkoy'
        storage = 'Windows DPAPI CurrentUser'
        identity_id = [string]$identity.id
        identity_name = [string]$identity.name
        identity_type = [string]$identity.type
        verified_endpoint = 'GET /v1/users/me'
        verified_at_utc = [DateTime]::UtcNow.ToString('o')
        ciphertext_sha256 = Get-Sha256Hex -Bytes $cipherBytes
        secret_path = $secretPath
    }

    $metadataTemp = "$metadataPath.$PID.tmp"
    $metadata | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $metadataTemp -Encoding utf8NoBOM
    Set-PrivateAcl -Path $metadataTemp
    Move-Item -LiteralPath $metadataTemp -Destination $metadataPath -Force
    Set-PrivateAcl -Path $metadataPath

    Write-Host ''
    Write-Host 'PAT validated and stored securely.' -ForegroundColor Green
    Write-Host "Profile: $profileKey"
    Write-Host "Workspace label: $workspaceName"
    Write-Host "Notion identity: $($identity.name)"
    Write-Host "Secret store: $secretPath"
    Write-Host 'No token value was printed or persisted in plaintext.'
}
catch {
    Write-Host ''
    Write-Host "Enrollment failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host 'No plaintext token was retained.'
    exit 1
}
finally {
    if ($plainBytes) {
        [Array]::Clear($plainBytes, 0, $plainBytes.Length)
    }
    if ($bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
    $plainToken = $null
    $secureToken = $null
    Remove-Variable headers -ErrorAction SilentlyContinue
}

Write-Host ''
Read-Host 'Press Enter to close this secure enrollment window' | Out-Null
