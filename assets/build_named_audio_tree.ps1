param(
    [string]$AudioRoot = '',
    [string]$MetaFile = '',
    [string]$SfxFile = '',
    [string]$StreamsFile = '',
    [string]$XmlFile = '',
    [string]$TreeRoot = '',
    [string]$BanksRoot = '',
    [string]$VgmstreamCli = '',
    [string[]]$IncludeBanks = @()
)

$ErrorActionPreference = 'Stop'

Add-Type -TypeDefinition @'
using System;
public static class WwiseHash {
    public static uint Fnv1Lower(string value) {
        uint hash = 2166136261;
        foreach (char ch in value.ToLowerInvariant()) {
            hash *= 16777619;
            hash ^= (byte)ch;
        }
        return hash;
    }
}
'@

function Get-SafeSegment {
    param(
        [string]$Value,
        [int]$MaxLength = 80
    )

    $clean = $Value
    $clean = $clean -replace '[<>:"/\\|?*]', '_'
    $clean = $clean -replace '\s+', ' '
    $clean = $clean.Trim(' ', '.')
    if ([string]::IsNullOrWhiteSpace($clean)) {
        $clean = 'unnamed'
    }
    if ($clean.Length -gt $MaxLength) {
        $suffix = '{0:X8}' -f [WwiseHash]::Fnv1Lower($clean)
        $keep = [Math]::Max(1, $MaxLength - $suffix.Length - 1)
        $clean = '{0}_{1}' -f $clean.Substring(0, $keep), $suffix
    }
    return $clean
}

function Ensure-Directory {
    param([string]$Path)
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Assert-PathWithinRoot {
    param(
        [string]$Candidate,
        [string]$Root
    )

    $candidatePath = [IO.Path]::GetFullPath($Candidate)
    $rootPath = [IO.Path]::GetFullPath($Root)
    if (-not $candidatePath.StartsWith($rootPath, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to clear unexpected path outside workspace root: $Candidate"
    }
}

function New-HardLinkSafe {
    param(
        [string]$Path,
        [string]$Target
    )

    if (Test-Path -LiteralPath $Path) {
        return
    }
    New-Item -ItemType HardLink -Path $Path -Target $Target | Out-Null
}

function Write-FileSlice {
    param(
        [string]$SourcePath,
        [int64]$Offset,
        [int64]$Length,
        [string]$DestinationPath
    )

    Ensure-Directory -Path (Split-Path -Parent $DestinationPath)
    $input = [IO.File]::OpenRead($SourcePath)
    try {
        $output = [IO.File]::Create($DestinationPath)
        try {
            $input.Seek($Offset, 'Begin') | Out-Null
            $buffer = New-Object byte[] (1MB)
            $remaining = $Length
            while ($remaining -gt 0) {
                $toRead = [Math]::Min($buffer.Length, $remaining)
                $read = $input.Read($buffer, 0, [int]$toRead)
                if ($read -le 0) {
                    throw "Unexpected EOF while copying slice from $SourcePath at offset $Offset"
                }
                $output.Write($buffer, 0, $read)
                $remaining -= $read
            }
        } finally {
            $output.Dispose()
        }
    } finally {
        $input.Dispose()
    }
}

function Ensure-DecodedFlatSource {
    param(
        $Info,
        [hashtable]$ArchiveFiles,
        [string]$VgmstreamCliPath,
        [string]$TempRoot
    )

    if (Test-Path -LiteralPath $Info.source) {
        return $true
    }
    if (-not $ArchiveFiles.ContainsKey($Info.archive)) {
        return $false
    }
    if (-not (Test-Path -LiteralPath $VgmstreamCliPath)) {
        return $false
    }

    $archivePath = $ArchiveFiles[$Info.archive]
    if (-not (Test-Path -LiteralPath $archivePath)) {
        return $false
    }

    Ensure-Directory -Path (Split-Path -Parent $Info.source)
    Ensure-Directory -Path $TempRoot

    $riffBytes = $null
    $fs = [IO.File]::OpenRead($archivePath)
    try {
        $offset = [int64]$Info.offset
        if (($offset + 12) -gt $fs.Length) {
            return $false
        }
        $fs.Seek($offset, 'Begin') | Out-Null
        $header = New-Object byte[] 12
        $read = $fs.Read($header, 0, $header.Length)
        if ($read -ne $header.Length) {
            return $false
        }
        if ([Text.Encoding]::ASCII.GetString($header, 0, 4) -ne 'RIFF') {
            $Info | Add-Member -NotePropertyName non_audio -NotePropertyValue $true -Force
            return $false
        }
        if ([Text.Encoding]::ASCII.GetString($header, 8, 4) -ne 'WAVE') {
            $Info | Add-Member -NotePropertyName non_audio -NotePropertyValue $true -Force
            return $false
        }
        $riffSize = [int64]([BitConverter]::ToUInt32($header, 4) + 8)
        if (($offset + $riffSize) -gt $fs.Length) {
            return $false
        }
        $fs.Seek($offset, 'Begin') | Out-Null
        $riffBytes = New-Object byte[] $riffSize
        $read = $fs.Read($riffBytes, 0, $riffBytes.Length)
        if ($read -ne $riffBytes.Length) {
            return $false
        }
    } finally {
        $fs.Dispose()
    }

    $tempBase = '{0}_{1}' -f $Info.archive, ('{0:X10}' -f [uint64]$Info.offset)
    $tempInput = Join-Path $TempRoot ($tempBase + '.wem')
    try {
        [IO.File]::WriteAllBytes($tempInput, $riffBytes)
        & $VgmstreamCliPath -o $Info.source $tempInput | Out-Null
        return (Test-Path -LiteralPath $Info.source)
    } finally {
        if (Test-Path -LiteralPath $tempInput) {
            Remove-Item -LiteralPath $tempInput -Force
        }
    }
}

function Find-AsciiOffsets {
    param(
        [string]$Path,
        [string]$Pattern
    )

    $bytes = [Text.Encoding]::ASCII.GetBytes($Pattern)
    $hits = [System.Collections.Generic.List[long]]::new()
    $fs = [IO.File]::OpenRead($Path)
    try {
        $chunkSize = 8MB
        $buffer = New-Object byte[] ($chunkSize + $bytes.Length - 1)
        $carry = 0
        $position = 0L
        while (($read = $fs.Read($buffer, $carry, $chunkSize)) -gt 0) {
            $total = $carry + $read
            for ($i = 0; $i -le $total - $bytes.Length; $i++) {
                $found = $true
                for ($j = 0; $j -lt $bytes.Length; $j++) {
                    if ($buffer[$i + $j] -ne $bytes[$j]) {
                        $found = $false
                        break
                    }
                }
                if ($found) {
                    $hits.Add($position + $i - $carry)
                }
            }
            if ($total -ge $bytes.Length - 1) {
                [Array]::Copy($buffer, $total - ($bytes.Length - 1), $buffer, 0, $bytes.Length - 1)
                $carry = $bytes.Length - 1
            } else {
                $carry = $total
            }
            $position += $read
        }
    } finally {
        $fs.Dispose()
    }
    return $hits
}

function Parse-ExternalPackIndex {
    param(
        [string]$File,
        [string]$Archive,
        [hashtable]$GlobalMedia
    )

    $entrySize = 0x98
    $fs = [IO.File]::OpenRead($File)
    try {
        $br = [IO.BinaryReader]::new($fs)
        $fs.Seek(0x88, 'Begin') | Out-Null
        $entryStart = [int64]$br.ReadUInt64()
        $entryCount = [int64]$br.ReadUInt64()
        for ($i = 0; $i -lt $entryCount; $i++) {
            $entryOffset = $entryStart + ($i * $entrySize)
            $fs.Seek($entryOffset + 128, 'Begin') | Out-Null
            $mediaId = $br.ReadUInt32()
            $null = $br.ReadUInt32()
            $riffOffsetLow = [uint64]$br.ReadUInt32()
            $riffOffsetHigh = [uint64]$br.ReadUInt32()
            $riffOffset = ($riffOffsetHigh -shl 32) -bor $riffOffsetLow
            $sizeLow = [uint64]$br.ReadUInt32()
            $sizeHigh = [uint64]$br.ReadUInt32()
            $size = ($sizeHigh -shl 32) -bor $sizeLow
            $flatPath = Join-Path $AudioRoot ('{0}\{0}.aesp_{1}.wav' -f $Archive, ('{0:X10}' -f $riffOffset))
            if (-not $GlobalMedia.ContainsKey([uint32]$mediaId)) {
                $GlobalMedia[[uint32]$mediaId] = [System.Collections.Generic.List[object]]::new()
            }
            $GlobalMedia[[uint32]$mediaId].Add([pscustomobject]@{
                archive = $Archive
                media_id = [uint32]$mediaId
                offset = [uint64]$riffOffset
                size = [uint64]$size
                source = $flatPath
                exists = (Test-Path -LiteralPath $flatPath)
            })
        }
    } finally {
        $fs.Dispose()
    }
}

function Parse-HircObjects {
    param(
        [IO.BinaryReader]$Reader,
        [long]$PayloadOffset
    )

    $objects = @{}
    $Reader.BaseStream.Seek($PayloadOffset, 'Begin') | Out-Null
    $count = $Reader.ReadUInt32()
    for ($i = 0; $i -lt $count; $i++) {
        $type = $Reader.ReadByte()
        $size = $Reader.ReadUInt32()
        $id = $Reader.ReadUInt32()
        $payload = $Reader.ReadBytes($size - 4)
        $objects[[uint32]$id] = [pscustomobject]@{
            type = [int]$type
            payload = $payload
        }
    }
    return $objects
}

function Get-EventActions {
    param([byte[]]$Payload)

    if ($Payload.Length -lt 1) {
        return @()
    }
    $count = [int]$Payload[0]
    if ($Payload.Length -lt (1 + ($count * 4))) {
        return @()
    }
    $actions = [System.Collections.Generic.List[uint32]]::new()
    for ($i = 0; $i -lt $count; $i++) {
        $actions.Add([BitConverter]::ToUInt32($Payload, 1 + ($i * 4)))
    }
    return $actions
}

function Get-ActionTarget {
    param(
        [byte[]]$Payload,
        [Collections.Generic.HashSet[uint32]]$KnownObjectIds
    )

    if ($Payload.Length -ge 6) {
        $target = [BitConverter]::ToUInt32($Payload, 2)
        if ($KnownObjectIds.Contains([uint32]$target)) {
            return [uint32]$target
        }
    }
    $targets = @(Get-AnyObjectRefs -Payload $Payload -KnownObjectIds $KnownObjectIds)
    if ($targets.Count -eq 1) {
        return [uint32]$targets[0]
    }
    return $null
}

function Get-TailChildren {
    param(
        [byte[]]$Payload,
        [Collections.Generic.HashSet[uint32]]$KnownObjectIds
    )

    $maxCount = [Math]::Min(512, [Math]::Floor(($Payload.Length - 4) / 4))
    for ($count = $maxCount; $count -ge 1; $count--) {
        $countOffset = $Payload.Length - 4 - ($count * 4)
        if ($countOffset -lt 0) {
            continue
        }
        $storedCount = [BitConverter]::ToUInt32($Payload, $countOffset)
        if ($storedCount -ne $count) {
            continue
        }
        $children = [System.Collections.Generic.List[uint32]]::new()
        $valid = $true
        for ($i = 0; $i -lt $count; $i++) {
            $child = [BitConverter]::ToUInt32($Payload, $countOffset + 4 + ($i * 4))
            if (-not $KnownObjectIds.Contains([uint32]$child)) {
                $valid = $false
                break
            }
            $children.Add([uint32]$child)
        }
        if ($valid) {
            return $children
        }
    }
    return @()
}

function Get-ActionLookupKeys {
    param([byte[]]$Payload)

    $keys = [System.Collections.Generic.List[uint32]]::new()
    if ($Payload.Length -ge 6) {
        $keys.Add([BitConverter]::ToUInt32($Payload, 2))
    }
    if ($Payload.Length -ge 13) {
        $keys.Add([BitConverter]::ToUInt32($Payload, 9))
    }
    if ($Payload.Length -ge 17) {
        $keys.Add([BitConverter]::ToUInt32($Payload, 13))
    }
    return $keys | Select-Object -Unique
}

function Get-AnyObjectRefs {
    param(
        [byte[]]$Payload,
        [Collections.Generic.HashSet[uint32]]$KnownObjectIds
    )

    $refs = [System.Collections.Generic.List[uint32]]::new()
    for ($offset = 0; $offset -le $Payload.Length - 4; $offset++) {
        $value = [BitConverter]::ToUInt32($Payload, $offset)
        if ($KnownObjectIds.Contains([uint32]$value)) {
            $refs.Add([uint32]$value)
        }
    }
    return $refs | Select-Object -Unique
}

function Get-StateMappedChildren {
    param(
        [hashtable]$LocalObjects,
        [uint32[]]$Keys,
        [Collections.Generic.HashSet[uint32]]$KnownObjectIds
    )

    $children = [System.Collections.Generic.List[uint32]]::new()
    foreach ($pair in $LocalObjects.GetEnumerator()) {
        $object = $pair.Value
        if ($object.type -notin 10, 12, 13) {
            continue
        }
        for ($offset = 0; $offset -le $object.payload.Length - 8; $offset++) {
            $value = [BitConverter]::ToUInt32($object.payload, $offset)
            if ($Keys -notcontains [uint32]$value) {
                continue
            }
            $childId = [BitConverter]::ToUInt32($object.payload, $offset + 4)
            if ($KnownObjectIds.Contains([uint32]$childId)) {
                $children.Add([uint32]$childId)
            }
        }
    }
    return $children | Select-Object -Unique
}

function Get-SoundMedia {
    param(
        [byte[]]$Payload,
        [hashtable]$GlobalMedia
    )

    $hits = [System.Collections.Generic.List[uint32]]::new()
    if ($Payload.Length -ge 9) {
        $primary = [BitConverter]::ToUInt32($Payload, 5)
        if ($GlobalMedia.ContainsKey([uint32]$primary)) {
            $hits.Add([uint32]$primary)
        }
    }
    if ($hits.Count -gt 0) {
        return $hits
    }
    for ($offset = 0; $offset -le $Payload.Length - 4; $offset++) {
        $value = [BitConverter]::ToUInt32($Payload, $offset)
        if ($GlobalMedia.ContainsKey([uint32]$value)) {
            $hits.Add([uint32]$value)
        }
    }
    return $hits | Select-Object -Unique
}

function Get-MusicTrackMedia {
    param(
        [byte[]]$Payload,
        [hashtable]$GlobalMedia
    )

    $hits = [System.Collections.Generic.List[uint32]]::new()
    foreach ($offset in 10, 27) {
        if ($Payload.Length -lt ($offset + 4)) {
            continue
        }
        $mediaId = [BitConverter]::ToUInt32($Payload, $offset)
        if ($GlobalMedia.ContainsKey([uint32]$mediaId)) {
            $hits.Add([uint32]$mediaId)
        }
    }
    return $hits | Select-Object -Unique
}

function Resolve-ObjectMedia {
    param(
        [uint32]$ObjectId,
        [hashtable]$LocalObjects,
        [hashtable]$GlobalObjects,
        [Collections.Generic.HashSet[uint32]]$KnownObjectIds,
        [hashtable]$GlobalMedia,
        [hashtable]$Memo,
        [Collections.Generic.HashSet[uint32]]$Stack
    )

    $key = [string]$ObjectId
    if ($Memo.ContainsKey($key)) {
        return $Memo[$key]
    }
    if (-not $Stack.Add($ObjectId)) {
        return @()
    }

    $object = $null
    if ($LocalObjects.ContainsKey($ObjectId)) {
        $object = $LocalObjects[$ObjectId]
    } elseif ($GlobalObjects.ContainsKey($ObjectId)) {
        $object = $GlobalObjects[$ObjectId]
    }

    $result = [System.Collections.Generic.List[uint32]]::new()
    if ($object) {
        switch ($object.type) {
            2 {
                foreach ($mediaId in (Get-SoundMedia -Payload $object.payload -GlobalMedia $GlobalMedia)) {
                    $result.Add([uint32]$mediaId)
                }
            }
            11 {
                foreach ($mediaId in (Get-MusicTrackMedia -Payload $object.payload -GlobalMedia $GlobalMedia)) {
                    $result.Add([uint32]$mediaId)
                }
            }
            3 {
                $target = Get-ActionTarget -Payload $object.payload -KnownObjectIds $KnownObjectIds
                if ($target) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $target -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
                if (
                    $result.Count -eq 0 -and
                    $object.payload.Length -ge 2 -and
                    $object.payload[1] -eq 0x12
                ) {
                    $lookupKeys = @(Get-ActionLookupKeys -Payload $object.payload)
                    foreach ($childId in (Get-StateMappedChildren -LocalObjects $LocalObjects -Keys $lookupKeys -KnownObjectIds $KnownObjectIds)) {
                        foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $childId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                            $result.Add([uint32]$mediaId)
                        }
                    }
                }
            }
            4 {
                foreach ($actionId in (Get-EventActions -Payload $object.payload)) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $actionId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
            }
            5 {
                $childIds = @(Get-TailChildren -Payload $object.payload -KnownObjectIds $KnownObjectIds)
                if ($childIds.Count -eq 0) {
                    $childIds = @(Get-AnyObjectRefs -Payload $object.payload -KnownObjectIds $KnownObjectIds)
                }
                foreach ($childId in $childIds) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $childId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
            }
            7 {
                foreach ($childId in (Get-TailChildren -Payload $object.payload -KnownObjectIds $KnownObjectIds)) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $childId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
            }
            10 {
                foreach ($childId in (Get-AnyObjectRefs -Payload $object.payload -KnownObjectIds $KnownObjectIds)) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $childId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
            }
            12 {
                foreach ($childId in (Get-AnyObjectRefs -Payload $object.payload -KnownObjectIds $KnownObjectIds)) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $childId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
            }
            13 {
                foreach ($childId in (Get-AnyObjectRefs -Payload $object.payload -KnownObjectIds $KnownObjectIds)) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $childId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
            }
            default {
                foreach ($childId in (Get-TailChildren -Payload $object.payload -KnownObjectIds $KnownObjectIds)) {
                    foreach ($mediaId in (Resolve-ObjectMedia -ObjectId $childId -LocalObjects $LocalObjects -GlobalObjects $GlobalObjects -KnownObjectIds $KnownObjectIds -GlobalMedia $GlobalMedia -Memo $Memo -Stack $Stack)) {
                        $result.Add([uint32]$mediaId)
                    }
                }
            }
        }
    }

    $Stack.Remove($ObjectId) | Out-Null
    $unique = $result | Select-Object -Unique
    $Memo[$key] = $unique
    return $unique
}

function Get-BankMediaLinks {
    param(
        $Bank,
        [hashtable]$PreloadsByName,
        [hashtable]$EventsByPreloadId,
        [hashtable]$GlobalObjects,
        [Collections.Generic.HashSet[uint32]]$KnownObjectIds,
        [hashtable]$GlobalMedia
    )

    if (-not $PreloadsByName.ContainsKey($Bank.name)) {
        return @()
    }
    $preload = $PreloadsByName[$Bank.name]
    if (-not $EventsByPreloadId.ContainsKey([string]$preload.id)) {
        return @()
    }

    $memo = @{}
    $links = [System.Collections.Generic.List[object]]::new()
    $localKnownObjectIds = [Collections.Generic.HashSet[uint32]]::new()
    foreach ($objectId in $Bank.objects.Keys) {
        [void]$localKnownObjectIds.Add([uint32]$objectId)
    }
    foreach ($eventName in $EventsByPreloadId[[string]$preload.id]) {
        $eventHash = [WwiseHash]::Fnv1Lower($eventName)
        if (-not $Bank.objects.ContainsKey([uint32]$eventHash)) {
            continue
        }
        $stack = [Collections.Generic.HashSet[uint32]]::new()
        $mediaIds = Resolve-ObjectMedia -ObjectId ([uint32]$eventHash) -LocalObjects $Bank.objects -GlobalObjects $GlobalObjects -KnownObjectIds $localKnownObjectIds -GlobalMedia $GlobalMedia -Memo $memo -Stack $stack
        foreach ($mediaId in ($mediaIds | Sort-Object)) {
            if (-not $GlobalMedia.ContainsKey([uint32]$mediaId)) {
                continue
            }
            $infos = @($GlobalMedia[[uint32]$mediaId])
            $existingInfos = @($infos | Where-Object { $_.exists })
            if ($existingInfos.Count -gt 0) {
                $infos = $existingInfos
            }
            foreach ($info in $infos) {
                $links.Add([pscustomobject]@{
                    bank = $Bank.name
                    event = [string]$eventName
                    media_id = [uint32]$mediaId
                    info = $info
                })
            }
        }
    }
    return $links
}

$xml = [xml](Get-Content -LiteralPath $XmlFile)
$preloadsByName = @{}
$eventsByPreloadId = @{}
foreach ($preload in $xml.Mapping.Preloads.Preload) {
    $preloadsByName[[string]$preload.name] = [pscustomobject]@{
        id = [uint32]$preload.id
        name = [string]$preload.name
        bank_hash = [WwiseHash]::Fnv1Lower([string]$preload.name)
    }
}
foreach ($event in $xml.Mapping.Events.Event) {
    $key = [string]$event.preload_id
    if (-not $eventsByPreloadId.ContainsKey($key)) {
        $eventsByPreloadId[$key] = [System.Collections.Generic.List[string]]::new()
    }
    $eventsByPreloadId[$key].Add([string]$event.name)
}

$bankIdToName = @{}
foreach ($preload in $preloadsByName.Values) {
    $bankIdToName[[uint32]$preload.bank_hash] = [string]$preload.name
}

$globalMedia = @{}
Parse-ExternalPackIndex -File $SfxFile -Archive 'sfx' -GlobalMedia $globalMedia
Parse-ExternalPackIndex -File $StreamsFile -Archive 'streams' -GlobalMedia $globalMedia
$archiveFiles = @{
    meta = $MetaFile
    sfx = $SfxFile
    streams = $StreamsFile
}
$decodeTempRoot = Join-Path $AudioRoot 'logs\temp_decode'

$bankOffsets = Find-AsciiOffsets -Path $MetaFile -Pattern 'BKHD'
$metaLength = (Get-Item -LiteralPath $MetaFile).Length
$metaFs = [IO.File]::OpenRead($MetaFile)
$metaReader = [IO.BinaryReader]::new($metaFs)

$banks = [System.Collections.Generic.List[object]]::new()
$extractedBanks = [System.Collections.Generic.List[object]]::new()
$globalObjects = @{}
$knownObjectIds = [Collections.Generic.HashSet[uint32]]::new()
$duplicateObjectIds = [System.Collections.Generic.List[uint32]]::new()

try {
    for ($i = 0; $i -lt $bankOffsets.Count; $i++) {
        $bankOffset = [int64]$bankOffsets[$i]
        $nextOffset = if ($i + 1 -lt $bankOffsets.Count) { [int64]$bankOffsets[$i + 1] } else { [int64]$metaLength }
        $position = $bankOffset
        $bankId = $null
        $bankName = $null
        $dataPayloadOffset = $null
        $didxEntries = [System.Collections.Generic.List[object]]::new()
        $hircObjects = @{}

        while (($position + 8) -le $nextOffset) {
            $metaFs.Seek($position, 'Begin') | Out-Null
            $chunkId = [Text.Encoding]::ASCII.GetString($metaReader.ReadBytes(4))
            $chunkLength = [int64]$metaReader.ReadUInt32()
            $payloadOffset = $position + 8
            if ($chunkLength -lt 0 -or ($payloadOffset + $chunkLength) -gt $nextOffset) {
                break
            }
            if ($chunkId -notmatch '^[A-Z0-9]{4}$') {
                break
            }

            switch ($chunkId) {
                'BKHD' {
                    $metaFs.Seek($payloadOffset, 'Begin') | Out-Null
                    $null = $metaReader.ReadUInt32()
                    $bankId = [uint32]$metaReader.ReadUInt32()
                    if ($bankIdToName.ContainsKey($bankId)) {
                        $bankName = $bankIdToName[$bankId]
                    } else {
                        $bankName = 'bank_{0:X8}' -f $bankId
                    }
                }
                'DIDX' {
                    $metaFs.Seek($payloadOffset, 'Begin') | Out-Null
                    $entryCount = [int]($chunkLength / 12)
                    for ($entryIndex = 0; $entryIndex -lt $entryCount; $entryIndex++) {
                        $didxEntries.Add([pscustomobject]@{
                            media_id = [uint32]$metaReader.ReadUInt32()
                            rel_offset = [uint32]$metaReader.ReadUInt32()
                            size = [uint32]$metaReader.ReadUInt32()
                        })
                    }
                }
                'DATA' {
                    $dataPayloadOffset = $payloadOffset
                }
                'HIRC' {
                    $hircObjects = Parse-HircObjects -Reader $metaReader -PayloadOffset $payloadOffset
                }
            }

            $position = $payloadOffset + $chunkLength
        }

        if ($bankName -and $dataPayloadOffset) {
            foreach ($entry in $didxEntries) {
                $riffOffset = $dataPayloadOffset + $entry.rel_offset
                $flatPath = Join-Path $AudioRoot ('meta\meta.aesp_{0}.wav' -f ('{0:X10}' -f $riffOffset))
                if (-not $globalMedia.ContainsKey([uint32]$entry.media_id)) {
                    $globalMedia[[uint32]$entry.media_id] = [System.Collections.Generic.List[object]]::new()
                }
                $globalMedia[[uint32]$entry.media_id].Add([pscustomobject]@{
                    archive = 'meta'
                    media_id = [uint32]$entry.media_id
                    offset = [uint32]$riffOffset
                    size = [uint32]$entry.size
                    source = $flatPath
                    exists = (Test-Path -LiteralPath $flatPath)
                })
            }
        }

        foreach ($pair in $hircObjects.GetEnumerator()) {
            $objectId = [uint32]$pair.Key
            [void]$knownObjectIds.Add($objectId)
            if (-not $globalObjects.ContainsKey($objectId)) {
                $globalObjects[$objectId] = $pair.Value
            } else {
                $duplicateObjectIds.Add($objectId)
            }
        }

        $banks.Add([pscustomobject]@{
            id = $bankId
            name = $bankName
            offset = $bankOffset
            length = ($nextOffset - $bankOffset)
            objects = $hircObjects
        })
    }
} finally {
    $metaReader.Dispose()
    $metaFs.Dispose()
}

if (Test-Path -LiteralPath $BanksRoot) {
    Assert-PathWithinRoot -Candidate $BanksRoot -Root $AudioRoot
    Remove-Item -LiteralPath $BanksRoot -Recurse -Force
}

if (Test-Path -LiteralPath $TreeRoot) {
    Assert-PathWithinRoot -Candidate $TreeRoot -Root $AudioRoot
    Remove-Item -LiteralPath $TreeRoot -Recurse -Force
}

Ensure-Directory -Path $BanksRoot
Ensure-Directory -Path $TreeRoot
Ensure-Directory -Path (Join-Path $AudioRoot 'logs')
$manifest = [System.Collections.Generic.List[object]]::new()
$unresolved = [System.Collections.Generic.List[object]]::new()

$bankFilterSet = $null
if ($IncludeBanks.Count -gt 0) {
    $bankFilterSet = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($bank in $IncludeBanks) {
        [void]$bankFilterSet.Add($bank)
    }
}

$bankNameCounts = @{}
foreach ($bank in $banks) {
    if ($bankFilterSet -and (-not $bankFilterSet.Contains($bank.name))) {
        continue
    }

    $exportBankName = if ([string]::IsNullOrWhiteSpace($bank.name)) {
        'unnamed_bank_{0:X10}' -f [uint64]$bank.offset
    } else {
        $bank.name
    }
    $safeBankName = Get-SafeSegment -Value $exportBankName -MaxLength 96
    if (-not $bankNameCounts.ContainsKey($safeBankName)) {
        $bankNameCounts[$safeBankName] = 0
    }
    $bankNameCounts[$safeBankName]++
    $leafName = if ($bankNameCounts[$safeBankName] -eq 1) {
        '{0}.bnk' -f $safeBankName
    } else {
        '{0}__{1:X8}__{2:X10}.bnk' -f $safeBankName, [uint32]$bank.id, [uint64]$bank.offset
    }
    $bankPath = Join-Path $BanksRoot $leafName
    Write-FileSlice -SourcePath $MetaFile -Offset ([int64]$bank.offset) -Length ([int64]$bank.length) -DestinationPath $bankPath
    $extractedBanks.Add([pscustomobject]@{
        bank = $exportBankName
        bank_id = [uint32]$bank.id
        offset = [uint64]$bank.offset
        length = [uint64]$bank.length
        path = $bankPath
    })
}

foreach ($bank in $banks) {
    if ([string]::IsNullOrWhiteSpace($bank.name) -or $bank.objects.Count -eq 0) {
        continue
    }
    if ($bankFilterSet -and (-not $bankFilterSet.Contains($bank.name))) {
        continue
    }

    $links = Get-BankMediaLinks -Bank $bank -PreloadsByName $preloadsByName -EventsByPreloadId $eventsByPreloadId -GlobalObjects $globalObjects -KnownObjectIds $knownObjectIds -GlobalMedia $globalMedia
    if ($links.Count -eq 0) {
        $unresolved.Add([pscustomobject]@{
            bank = $bank.name
            note = 'No mapped event media found'
        })
        continue
    }

    $grouped = $links | Group-Object bank, event
    foreach ($group in $grouped) {
        $first = $group.Group[0]
        $bankFolder = Get-SafeSegment -Value $first.bank -MaxLength 64
        $eventFolder = Get-SafeSegment -Value $first.event -MaxLength 96
        $perArchive = $group.Group | Group-Object { $_.info.archive }
        foreach ($archiveGroup in $perArchive) {
            $archiveName = [string]$archiveGroup.Name
            $eventDir = Join-Path $TreeRoot (Join-Path $archiveName (Join-Path $bankFolder $eventFolder))
            $eventDirCreated = $false
            $sortedMedia = $archiveGroup.Group | Sort-Object media_id
            for ($index = 0; $index -lt $sortedMedia.Count; $index++) {
                $entry = $sortedMedia[$index]
                $leaf = if ($sortedMedia.Count -eq 1) {
                    'media_{0}.wav' -f $entry.media_id
                } else {
                    '{0:D3}__media_{1}.wav' -f ($index + 1), $entry.media_id
                }
                $targetPath = Join-Path $eventDir $leaf
                if (-not (Test-Path -LiteralPath $entry.info.source)) {
                    $restored = Ensure-DecodedFlatSource -Info $entry.info -ArchiveFiles $archiveFiles -VgmstreamCliPath $VgmstreamCli -TempRoot $decodeTempRoot
                    if ($restored) {
                        $entry.info.exists = $true
                    }
                }
                if ($entry.info.PSObject.Properties.Name -contains 'non_audio' -and $entry.info.non_audio) {
                    continue
                }
                if (-not (Test-Path -LiteralPath $entry.info.source)) {
                    $unresolved.Add([pscustomobject]@{
                        bank = $entry.bank
                        event = $entry.event
                        media_id = $entry.media_id
                        note = "Missing flat source file: $($entry.info.source)"
                    })
                    continue
                }
                if (-not $eventDirCreated) {
                    Ensure-Directory -Path $eventDir
                    $eventDirCreated = $true
                }
                New-HardLinkSafe -Path $targetPath -Target $entry.info.source
                $manifest.Add([pscustomobject]@{
                    archive = $archiveName
                    bank = $entry.bank
                    event = $entry.event
                    media_id = $entry.media_id
                    source = $entry.info.source
                    link = $targetPath
                })
            }
        }
    }
}

$manifestPath = Join-Path $AudioRoot 'logs\named_tree_manifest.csv'
$unresolvedPath = Join-Path $AudioRoot 'logs\named_tree_unresolved.csv'
$summaryPath = Join-Path $AudioRoot 'logs\named_tree_summary.txt'
$banksManifestPath = Join-Path $AudioRoot 'logs\extracted_banks_manifest.csv'

$manifest | Sort-Object archive, bank, event, media_id | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $manifestPath
$unresolved | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $unresolvedPath
$extractedBanks | Sort-Object bank, bank_id, offset | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $banksManifestPath

$summaryLines = [System.Collections.Generic.List[string]]::new()
$summaryLines.Add("Tree root: $TreeRoot")
$summaryLines.Add("Banks root: $BanksRoot")
$summaryLines.Add("Banks parsed: $($banks.Count)")
$summaryLines.Add("Banks extracted: $($extractedBanks.Count)")
$summaryLines.Add("Global media entries: $($globalMedia.Count)")
$summaryLines.Add("Global objects: $($globalObjects.Count)")
$summaryLines.Add("Duplicate object IDs seen: $($duplicateObjectIds.Count)")
$summaryLines.Add("Links created: $($manifest.Count)")
$summaryLines.Add("Unresolved notes: $($unresolved.Count)")
$summaryLines.Add('')
foreach ($archiveName in 'meta', 'sfx', 'streams') {
    $count = ($manifest | Where-Object { $_.archive -eq $archiveName } | Measure-Object).Count
    $summaryLines.Add(('{0}: {1} links' -f $archiveName, $count))
}
Set-Content -Path $summaryPath -Value $summaryLines -Encoding UTF8

$summaryLines -join [Environment]::NewLine
