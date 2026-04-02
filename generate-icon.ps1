# generate-icon.ps1
# Renders the official Copilot octicon via WPF and writes a PNG-in-ICO file.
# Must be invoked with: powershell.exe -Sta -NoProfile -ExecutionPolicy Bypass -File generate-icon.ps1
#
# PNG-in-ICO format is supported on Windows Vista and later.
# The resulting .ico is used as the ApplicationIcon embedded in the EXE.

param(
	[string]$OutputPath = "Resources\copilot_icon.ico"
)

Add-Type -AssemblyName WindowsBase
Add-Type -AssemblyName PresentationCore

# Same path data as in App.xaml CopilotIcon DrawingImage resource
$pathData = @'
F1 M23.922 16.992c-.861 1.495-5.859 5.023-11.922 5.023-6.063 0-11.061-3.528-11.922-5.023A.641.641 0 0 1 0 16.736v-2.869a.841.841 0 0 1 .053-.22c.372-.935 1.347-2.292 2.605-2.656.167-.429.414-1.055.644-1.517a10.195 10.195 0 0 1-.052-1.086c0-1.331.282-2.499 1.132-3.368.397-.406.89-.717 1.474-.952 1.399-1.136 3.392-2.093 6.122-2.093 2.731 0 4.767.957 6.166 2.093.584.235 1.077.546 1.474.952.85.869 1.132 2.037 1.132 3.368 0 .368-.014.733-.052 1.086.23.462.477 1.088.644 1.517 1.258.364 2.233 1.721 2.605 2.656a.832.832 0 0 1 .053.22v2.869a.641.641 0 0 1-.078.256ZM12.172 11h-.344a4.323 4.323 0 0 1-.355.508C10.703 12.455 9.555 13 7.965 13c-1.725 0-2.989-.359-3.782-1.259a2.005 2.005 0 0 1-.085-.104L4 11.741v6.585c1.435.779 4.514 2.179 8 2.179 3.486 0 6.565-1.4 8-2.179v-6.585l-.098-.104s-.033.045-.085.104c-.793.9-2.057 1.259-3.782 1.259-1.59 0-2.738-.545-3.508-1.492a4.323 4.323 0 0 1-.355-.508h-.016.016Zm.641-2.935c.136 1.057.403 1.913.878 2.497.442.544 1.134.938 2.344.938 1.573 0 2.292-.337 2.657-.751.384-.435.558-1.15.558-2.361 0-1.14-.243-1.847-.705-2.319-.477-.488-1.319-.862-2.824-1.025-1.487-.161-2.192.138-2.533.529-.269.307-.437.808-.438 1.578v.021c0 .265.021.562.063.893Zm-1.626 0c.042-.331.063-.628.063-.894v-.02c-.001-.77-.169-1.271-.438-1.578-.341-.391-1.046-.69-2.533-.529-1.505.163-2.347.537-2.824 1.025-.462.472-.705 1.179-.705 2.319 0 1.211.175 1.926.558 2.361.365.414 1.084.751 2.657.751 1.21 0 1.902-.394 2.344-.938.475-.584.742-1.44.878-2.497Z M14.5 14.25a1 1 0 0 1 1 1v2a1 1 0 0 1-2 0v-2a1 1 0 0 1 1-1Zm-5 0a1 1 0 0 1 1 1v2a1 1 0 0 1-2 0v-2a1 1 0 0 1 1-1Z
'@.Trim()

function Get-PngBytes([int]$size)
{
	$bgGeom = New-Object System.Windows.Media.RectangleGeometry
	$bgGeom.Rect = New-Object System.Windows.Rect(0, 0, 24, 24)
	$bgGeom.RadiusX = 4
	$bgGeom.RadiusY = 4
	$bgBrush = New-Object System.Windows.Media.SolidColorBrush(
		[System.Windows.Media.Color]::FromRgb(0x1C, 0x21, 0x28))
	$bgDrawing = New-Object System.Windows.Media.GeometryDrawing
	$bgDrawing.Brush = $bgBrush
	$bgDrawing.Geometry = $bgGeom

	$iconGeom = [System.Windows.Media.Geometry]::Parse($pathData)
	$iconDrawing = New-Object System.Windows.Media.GeometryDrawing
	$iconDrawing.Brush = [System.Windows.Media.Brushes]::White
	$iconDrawing.Geometry = $iconGeom

	$group = New-Object System.Windows.Media.DrawingGroup
	[void]$group.Children.Add($bgDrawing)
	[void]$group.Children.Add($iconDrawing)

	$visual = New-Object System.Windows.Media.DrawingVisual
	$dc = $visual.RenderOpen()
	$scale = [double]$size / 24.0
	$dc.PushTransform((New-Object System.Windows.Media.ScaleTransform($scale, $scale)))
	$dc.DrawDrawing($group)
	$dc.Pop()
	$dc.Close()

	$rtb = New-Object System.Windows.Media.Imaging.RenderTargetBitmap(
		$size, $size, 96.0, 96.0,
		[System.Windows.Media.PixelFormats]::Pbgra32)
	$rtb.Render($visual)

	$encoder = New-Object System.Windows.Media.Imaging.PngBitmapEncoder
	$encoder.Frames.Add([System.Windows.Media.Imaging.BitmapFrame]::Create($rtb))
	$ms = New-Object System.IO.MemoryStream
	$encoder.Save($ms)
	return , $ms.ToArray()
}

Write-Host "Generating Copilot icon: $OutputPath"

$png16 = Get-PngBytes 16
$png32 = Get-PngBytes 32
$pngs = @($png16, $png32)
$sizes = @(16, 32)
$n = $pngs.Length

$dir = [System.IO.Path]::GetDirectoryName($OutputPath)
if ($dir -and -not [System.IO.Directory]::Exists($dir))
{
	[System.IO.Directory]::CreateDirectory($dir) | Out-Null
}

$stream = [System.IO.File]::Open($OutputPath, [System.IO.FileMode]::Create)
$w = New-Object System.IO.BinaryWriter($stream)

# ICONDIR header
$w.Write([uint16]0)		# reserved
$w.Write([uint16]1)		# type: 1 = ICO
$w.Write([uint16]$n)	# number of images

# Offset to first image data: 6 bytes header + 16 bytes per entry
$offset = [uint32](6 + 16 * $n)

# ICONDIRENTRY for each image
for ($i = 0; $i -lt $n; $i++)
{
	$sz = $sizes[$i]
	$pngLen = [uint32]$pngs[$i].Length
	$w.Write([byte]$sz)		# width
	$w.Write([byte]$sz)		# height
	$w.Write([byte]0)		# color count (0 = no palette)
	$w.Write([byte]0)		# reserved
	$w.Write([uint16]0)		# color planes (0 for PNG entries)
	$w.Write([uint16]32)	# bits per pixel
	$w.Write($pngLen)		# size of image data
	$w.Write($offset)		# offset to image data
	$offset += $pngLen
}

# PNG image data
foreach ($pngData in $pngs)
{
	$w.Write($pngData, 0, $pngData.Length)
}

$w.Close()
$stream.Close()

Write-Host "Done: $OutputPath ($($png16.Length) + $($png32.Length) bytes)"
