using System.Drawing;
using System.Drawing.Drawing2D;
using System.Runtime.InteropServices;
using DrawingColor = System.Drawing.Color;

namespace CopilotUsage.Helpers;

/// <summary>
/// Selects the visual style used to show usage in the tray icon.
/// Change <see cref="TrayIconHelper.CurrentStyle"/> and rebuild to compare styles.
/// </summary>
internal enum IconVisualization
{
	/// <summary>The icon is filled from the bottom upward with the usage colour.</summary>
	LiquidFill,

	/// <summary>A pie/donut sector sweeps clockwise from 12 o'clock proportional to usage.</summary>
	Pie,

	/// <summary>A thick coloured line traces the icon perimeter clockwise from the top-left corner.</summary>
	BorderProgress,
}

internal static class TrayIconHelper
{
	private const int IconSize = 32;
	private const double SvgViewBox = 24.0;

	// Corner radius of the icon background, matching the WPF resource (4 / 24 * 32 ≈ 5.3)
	private const float CornerRadius = 5.3f;

	// ── Change this constant and rebuild to switch between visualization styles ──
	public const IconVisualization CurrentStyle = IconVisualization.BorderProgress;

	// LiquidFill constants
	private const int TintAlpha = 70;

	// BorderProgress constants
	private const float BorderWidth = 4.0f;

	// Pie constants — donut ring dimensions (outer fills icon minus 1px margin, inner leaves logo visible)
	private const float PieOuter = IconSize - 2f;
	private const float PieInner = 14f;

	private static Bitmap? s_CopilotBase;
	private static Bitmap? s_CopilotPaths;


	/// <summary>
	/// Creates the tray icon for the given usage percentage using <see cref="CurrentStyle"/>.
	/// Pass <c>null</c> when data is unavailable.
	/// </summary>
	public static Icon CreateUsageIcon( double? usagePercent )
	{
		return CurrentStyle switch
		{
			IconVisualization.Pie => CreatePieIcon( usagePercent ),
			IconVisualization.BorderProgress => CreateBorderProgressIcon( usagePercent ),
			_ => CreateLiquidFillIcon( usagePercent ),
		};
	}


	// ── Liquid fill ──────────────────────────────────────────────────────────────

	/// <summary>
	/// Four layers: dark bg → opaque colour fill from bottom → Copilot paths → full tint overlay.
	/// </summary>
	private static Icon CreateLiquidFillIcon( double? usagePercent )
	{
		using var bitmap = new Bitmap( IconSize, IconSize, System.Drawing.Imaging.PixelFormat.Format32bppArgb );
		using var g = Graphics.FromImage( bitmap );
		g.SmoothingMode = SmoothingMode.AntiAlias;

		DrawBackground( g );

		var fillRgb = GetFillColorRgb( usagePercent );
		var fillHeight = usagePercent.HasValue
			? (int) Math.Round( usagePercent.Value / 100.0 * IconSize )
			: IconSize;

		if ( fillHeight > 0 )
		{
			using var fillPath = RoundedRect( 0, 0, IconSize, IconSize, CornerRadius );
			g.SetClip( fillPath );
			using ( var fillBrush = new SolidBrush( fillRgb ) )
			{
				g.FillRectangle( fillBrush, 0, IconSize - fillHeight, IconSize, fillHeight );
			}
			g.ResetClip();
		}

		DrawCopilotPaths( g );

		var tintColor = DrawingColor.FromArgb( TintAlpha, fillRgb.R, fillRgb.G, fillRgb.B );
		using ( var tintPath = RoundedRect( 0, 0, IconSize, IconSize, CornerRadius ) )
		using ( var tintBrush = new SolidBrush( tintColor ) )
		{
			g.FillPath( tintBrush, tintPath );
		}

		return BitmapToIcon( bitmap );
	}


	// ── Pie / donut ──────────────────────────────────────────────────────────────

	/// <summary>
	/// Three layers: dark bg → donut sector sweep from 12 o'clock → Copilot paths on top.
	/// The donut ring leaves the central logo area unobscured.
	/// </summary>
	private static Icon CreatePieIcon( double? usagePercent )
	{
		using var bitmap = new Bitmap( IconSize, IconSize, System.Drawing.Imaging.PixelFormat.Format32bppArgb );
		using var g = Graphics.FromImage( bitmap );
		g.SmoothingMode = SmoothingMode.AntiAlias;

		DrawBackground( g );

		var fillRgb = GetFillColorRgb( usagePercent );
		float sweep = usagePercent.HasValue ? (float) ( usagePercent.Value * 3.6 ) : 360f;

		if ( sweep > 0 )
		{
			// Build an annular (donut) sector: outer pie minus inner circle
			float outerMargin = ( IconSize - PieOuter ) / 2f;
			float innerMargin = ( IconSize - PieInner ) / 2f;

			using var sectorPath = new GraphicsPath();
			// Outer arc: -90° (top) clockwise
			sectorPath.AddArc( outerMargin, outerMargin, PieOuter, PieOuter, -90f, sweep );
			// Inner arc: reverse direction to create the hole
			sectorPath.AddArc( innerMargin, innerMargin, PieInner, PieInner,
				-90f + sweep, -sweep );
			sectorPath.CloseFigure();

			using var fillBrush = new SolidBrush( fillRgb );
			g.FillPath( fillBrush, sectorPath );
		}

		DrawCopilotPaths( g );

		return BitmapToIcon( bitmap );
	}


	// ── Border progress ──────────────────────────────────────────────────────────

	/// <summary>
	/// Three layers: dark bg → Copilot paths → thick progress line tracing the perimeter clockwise.
	/// The Copilot logo is completely unobscured.
	/// </summary>
	private static Icon CreateBorderProgressIcon( double? usagePercent )
	{
		using var bitmap = new Bitmap( IconSize, IconSize, System.Drawing.Imaging.PixelFormat.Format32bppArgb );
		using var g = Graphics.FromImage( bitmap );
		g.SmoothingMode = SmoothingMode.AntiAlias;

		DrawBackground( g );
		DrawCopilotPaths( g );

		var fillRgb = GetFillColorRgb( usagePercent );
		// Total perimeter of a square (measured along the centre of the BorderWidth-wide stroke)
		float half = BorderWidth / 2f;
		float side = IconSize - BorderWidth;
		// The four corners of the stroke centre-line (inset by half the stroke width)
		var topLeft = new PointF( half, half );
		var topRight = new PointF( IconSize - half, half );
		var bottomRight = new PointF( IconSize - half, IconSize - half );
		var bottomLeft = new PointF( half, IconSize - half );

		// Perimeter segments in clockwise order starting from top-left
		var segments = new (PointF From, PointF To)[]
		{
			( topLeft,     topRight    ),  // top edge    → 0–25 %
			( topRight,    bottomRight ),  // right edge  → 25–50 %
			( bottomRight, bottomLeft  ),  // bottom edge → 50–75 %
			( bottomLeft,  topLeft     ),  // left edge   → 75–100 %
		};

		float totalPerimeter = side * 4f;
		float progressLength = usagePercent.HasValue
			? (float) ( usagePercent.Value / 100.0 * totalPerimeter )
			: totalPerimeter;

		if ( progressLength <= 0 )
		{
			return BitmapToIcon( bitmap );
		}

		using var pen = new Pen( fillRgb, BorderWidth )
		{
			StartCap = LineCap.Round,
			EndCap = LineCap.Round,
		};

		float remaining = progressLength;
		foreach ( var (from, to) in segments )
		{
			if ( remaining <= 0 )
			{
				break;
			}

			float segLen = Distance( from, to );
			if ( remaining >= segLen )
			{
				// Draw the full segment
				g.DrawLine( pen, from, to );
				remaining -= segLen;
			}
			else
			{
				// Draw a partial segment
				float t = remaining / segLen;
				var partial = new PointF(
					from.X + ( to.X - from.X ) * t,
					from.Y + ( to.Y - from.Y ) * t );
				g.DrawLine( pen, from, partial );
				remaining = 0;
			}
		}

		return BitmapToIcon( bitmap );
	}

	private static float Distance( PointF a, PointF b )
	{
		float dx = b.X - a.X;
		float dy = b.Y - a.Y;
		return (float) Math.Sqrt( dx * dx + dy * dy );
	}


	// ── Shared helpers ───────────────────────────────────────────────────────────

	private static void DrawBackground( Graphics g )
	{
		var bgColor = DrawingColor.FromArgb( 28, 33, 40 );
		using var bgBrush = new SolidBrush( bgColor );
		using var bgPath = RoundedRect( 0, 0, IconSize, IconSize, CornerRadius );
		g.FillPath( bgBrush, bgPath );
	}

	private static void DrawCopilotPaths( Graphics g )
	{
		var pathsBmp = GetCopilotPaths();
		if ( pathsBmp != null )
		{
			g.DrawImage( pathsBmp, 0, 0, IconSize, IconSize );
		}
	}

	private static Icon BitmapToIcon( Bitmap bitmap )
	{
		var hIcon = bitmap.GetHicon();
		try
		{
			return (Icon) Icon.FromHandle( hIcon ).Clone();
		}
		finally
		{
			DestroyIcon( hIcon );
		}
	}

	private static DrawingColor GetFillColorRgb( double? usagePercent )
	{
		if ( !usagePercent.HasValue )
		{
			return DrawingColor.FromArgb( 108, 117, 125 );  // grey — error/unknown
		}

		return usagePercent.Value switch
		{
			< 60 => DrawingColor.FromArgb( 40, 167, 69 ),   // green
			< 80 => DrawingColor.FromArgb( 255, 193, 7 ),   // amber
			_ => DrawingColor.FromArgb( 220, 53, 69 ),      // red
		};
	}

	/// <summary>Builds a GraphicsPath for a rounded rectangle.</summary>
	private static GraphicsPath RoundedRect( float x, float y, float w, float h, float r )
	{
		var path = new GraphicsPath();
		float d = r * 2;
		path.AddArc( x, y, d, d, 180, 90 );
		path.AddArc( x + w - d, y, d, d, 270, 90 );
		path.AddArc( x + w - d, y + h - d, d, d, 0, 90 );
		path.AddArc( x, y + h - d, d, d, 90, 90 );
		path.CloseFigure();
		return path;
	}


	/// <summary>
	/// Renders only the white Copilot geometry (no background) from the WPF resource.
	/// The resulting bitmap has a transparent background so it can be composed on top.
	/// Result is cached for the lifetime of the application.
	/// </summary>
	private static Bitmap? GetCopilotPaths()
	{
		if ( s_CopilotPaths != null )
		{
			return s_CopilotPaths;
		}

		var app = System.Windows.Application.Current;
		if ( app?.Resources["CopilotIcon"] is not System.Windows.Media.DrawingImage drawingImage )
		{
			return null;
		}

		// The DrawingImage contains a DrawingGroup with two children:
		//   [0] background RectangleGeometry (dark navy) — skip this
		//   [1] white path GeometryDrawing — render only this
		if ( drawingImage.Drawing is not System.Windows.Media.DrawingGroup group
			|| group.Children.Count < 2 )
		{
			return null;
		}

		var pathsDrawing = group.Children[1];

		var visual = new System.Windows.Media.DrawingVisual();
		using ( var dc = visual.RenderOpen() )
		{
			double scale = IconSize / SvgViewBox;
			dc.PushTransform( new System.Windows.Media.ScaleTransform( scale, scale ) );
			dc.DrawDrawing( pathsDrawing );
			dc.Pop();
		}

		var rtb = new System.Windows.Media.Imaging.RenderTargetBitmap(
			IconSize, IconSize, 96, 96,
			System.Windows.Media.PixelFormats.Pbgra32 );
		rtb.Render( visual );

		var encoder = new System.Windows.Media.Imaging.PngBitmapEncoder();
		encoder.Frames.Add( System.Windows.Media.Imaging.BitmapFrame.Create( rtb ) );
		using var stream = new System.IO.MemoryStream();
		encoder.Save( stream );
		stream.Position = 0;
		s_CopilotPaths = new Bitmap( stream );
		return s_CopilotPaths;
	}


	/// <summary>
	/// Renders the full "CopilotIcon" DrawingImage (background + paths) to a GDI+ Bitmap.
	/// Used by <see cref="GetWpfImageSource"/> for About / window title-bar icons.
	/// Result is cached for the lifetime of the application.
	/// </summary>
	private static Bitmap? GetCopilotBase()
	{
		if ( s_CopilotBase != null )
		{
			return s_CopilotBase;
		}

		var app = System.Windows.Application.Current;
		if ( app?.Resources["CopilotIcon"] is not System.Windows.Media.DrawingImage drawingImage )
		{
			return null;
		}

		var visual = new System.Windows.Media.DrawingVisual();
		using ( var dc = visual.RenderOpen() )
		{
			double scale = IconSize / SvgViewBox;
			dc.PushTransform( new System.Windows.Media.ScaleTransform( scale, scale ) );
			dc.DrawDrawing( drawingImage.Drawing );
			dc.Pop();
		}

		var rtb = new System.Windows.Media.Imaging.RenderTargetBitmap(
			IconSize, IconSize, 96, 96,
			System.Windows.Media.PixelFormats.Pbgra32 );
		rtb.Render( visual );

		var encoder = new System.Windows.Media.Imaging.PngBitmapEncoder();
		encoder.Frames.Add( System.Windows.Media.Imaging.BitmapFrame.Create( rtb ) );
		using var stream = new System.IO.MemoryStream();
		encoder.Save( stream );
		stream.Position = 0;
		s_CopilotBase = new Bitmap( stream );
		return s_CopilotBase;
	}


	[DllImport( "user32.dll" )]
	private static extern bool DestroyIcon( IntPtr handle );

	[DllImport( "gdi32.dll" )]
	private static extern bool DeleteObject( IntPtr hObject );


	/// <summary>
	/// Returns a WPF <see cref="System.Windows.Media.Imaging.BitmapSource"/> of the Copilot icon
	/// for use as <c>Window.Icon</c> or an <c>Image.Source</c>.
	/// </summary>
	public static System.Windows.Media.Imaging.BitmapSource? GetWpfImageSource()
	{
		var bmp = GetCopilotBase();
		if ( bmp == null )
		{
			return null;
		}

		var hBmp = bmp.GetHbitmap();
		try
		{
			return System.Windows.Interop.Imaging.CreateBitmapSourceFromHBitmap(
				hBmp,
				IntPtr.Zero,
				System.Windows.Int32Rect.Empty,
				System.Windows.Media.Imaging.BitmapSizeOptions.FromEmptyOptions() );
		}
		finally
		{
			DeleteObject( hBmp );
		}
	}
}

