using CopilotUsage.Helpers;
using System.Diagnostics;
using System.Windows;
using System.Windows.Navigation;

namespace CopilotUsage.Views;

public partial class AboutWindow : Window
{
	public AboutWindow()
	{
		InitializeComponent();
		var icon = TrayIconHelper.GetWpfImageSource();
		if ( icon != null )
		{
			Icon = icon;
		}
	}

	private void OkButton_Click( object sender, RoutedEventArgs e )
	{
		Close();
	}

	private void Hyperlink_RequestNavigate( object sender, RequestNavigateEventArgs e )
	{
		Process.Start( new ProcessStartInfo( e.Uri.AbsoluteUri ) { UseShellExecute = true } );
		e.Handled = true;
	}
}
