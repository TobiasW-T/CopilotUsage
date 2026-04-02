using CopilotUsage.Helpers;
using CopilotUsage.Services;
using CopilotUsage.Views;

namespace CopilotUsage;

public partial class App : System.Windows.Application
{
	private static Mutex? s_SingleInstanceMutex;
	private TrayApplicationContext? m_TrayContext;


	protected override void OnStartup( System.Windows.StartupEventArgs e )
	{
		base.OnStartup( e );

		s_SingleInstanceMutex = new Mutex( true, "CopilotUsage_SingleInstance", out var isNewInstance );
		if ( !isNewInstance )
		{
			Shutdown();
			return;
		}

		bool isDemoMode = e.Args.Length > 0 && e.Args[0].Equals( "--demo", StringComparison.OrdinalIgnoreCase );

		if ( !isDemoMode )
		{
			var settings = SettingsService.Load();

			// If no token yet, show settings so the user can authorize
			if ( string.IsNullOrWhiteSpace( settings.AccessToken ) )
			{
				var settingsWindow = new SettingsWindow();
				if ( settingsWindow.ShowDialog() != true )
				{
					Shutdown();
					return;
				}
			}

			StartupHelper.ApplyStartupSetting( SettingsService.Load().StartWithWindows );
		}

		m_TrayContext = new TrayApplicationContext( new GitHubCopilotService(), isDemoMode );
	}


	protected override void OnExit( System.Windows.ExitEventArgs e )
	{
		m_TrayContext?.Dispose();
		s_SingleInstanceMutex?.ReleaseMutex();
		s_SingleInstanceMutex?.Dispose();
		base.OnExit( e );
	}
}
