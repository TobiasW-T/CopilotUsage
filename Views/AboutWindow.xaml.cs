using CopilotUsage.Helpers;
using System.Windows;

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
}
