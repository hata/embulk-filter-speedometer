Embulk::JavaPlugin.register_filter(
  "speedometer", "org.embulk.filter.SpeedometerFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
