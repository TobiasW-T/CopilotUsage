using System.IO;
using System.Text.Json;
using CopilotUsage.Models;

namespace CopilotUsage.Services;

internal static class SettingsService
{
	private static readonly string SettingsDirectory =
		Path.Combine( Environment.GetFolderPath( Environment.SpecialFolder.ApplicationData ), "CopilotUsage" );

	private static readonly string SettingsFilePath =
		Path.Combine( SettingsDirectory, "settings.json" );

	private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };


	public static AppSettings Load()
	{
		try
		{
			if ( !File.Exists( SettingsFilePath ) )
			{
				return new AppSettings();
			}

			var json = File.ReadAllText( SettingsFilePath );
			return JsonSerializer.Deserialize<AppSettings>( json ) ?? new AppSettings();
		}
		catch
		{
			return new AppSettings();
		}
	}

	public static void Save( AppSettings settings )
	{
		Directory.CreateDirectory( SettingsDirectory );
		var json = JsonSerializer.Serialize( settings, JsonOptions );
		File.WriteAllText( SettingsFilePath, json );
	}
}
