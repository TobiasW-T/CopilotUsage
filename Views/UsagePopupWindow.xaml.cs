using System.ComponentModel;
using System.Windows;
using System.Windows.Input;
using System.Windows.Threading;
using CopilotUsage.ViewModels;

namespace CopilotUsage.Views;

public partial class UsagePopupWindow : Window
{
	internal UsageViewModel ViewModel { get; }

	// Timer ticks ~30 fps and rotates the spinner arc by 12° per tick (one full rotation per 0.8 s).
	// It is started/stopped by the IsLoading property so it consumes zero CPU when not refreshing.
	private readonly DispatcherTimer m_SpinnerTimer;
	private double m_SpinnerAngle;


	internal UsagePopupWindow( UsageViewModel viewModel )
	{
		ViewModel = viewModel;
		DataContext = viewModel;
		InitializeComponent();

		m_SpinnerTimer = new DispatcherTimer( DispatcherPriority.Render )
		{
			Interval = TimeSpan.FromMilliseconds( 33 ),
		};
		m_SpinnerTimer.Tick += SpinnerTimer_Tick;

		viewModel.PropertyChanged += ViewModel_PropertyChanged;
		Closed += Window_Closed;

		// Position once the layout pass has run and the actual size is known.
		Loaded += ( _, _ ) => PositionNearTray();
	}


	private void ViewModel_PropertyChanged( object? sender, PropertyChangedEventArgs e )
	{
		if ( e.PropertyName != nameof( UsageViewModel.IsLoading ) )
		{
			return;
		}

		if ( ViewModel.IsLoading )
		{
			m_SpinnerTimer.Start();
		}
		else
		{
			m_SpinnerTimer.Stop();
		}
	}

	private void SpinnerTimer_Tick( object? sender, EventArgs e )
	{
		m_SpinnerAngle = ( m_SpinnerAngle + 12.0 ) % 360.0;
		SpinnerRotation.Angle = m_SpinnerAngle;
	}

	private void Window_Closed( object? sender, EventArgs e )
	{
		m_SpinnerTimer.Stop();
		ViewModel.PropertyChanged -= ViewModel_PropertyChanged;
	}


	private void PositionNearTray()
	{
		var workArea = SystemParameters.WorkArea;
		Left = workArea.Right - Width - 12;
		Top = workArea.Bottom - Height - 12;
	}

	private void Border_MouseLeftButtonDown( object sender, MouseButtonEventArgs e )
	{
		// Allow the user to drag the window from anywhere on its surface
		// (button clicks are already handled/marked handled by the button itself).
		if ( e.ButtonState == MouseButtonState.Pressed )
		{
			DragMove();
		}
	}


	private bool m_IsClosing;

	private void CloseWindow()
	{
		if ( m_IsClosing )
		{
			return;
		}

		m_IsClosing = true;
		Close();
	}

	private void CloseButton_Click( object sender, RoutedEventArgs e )
	{
		CloseWindow();
	}
}
