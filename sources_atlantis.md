- [Traffic status](https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName%20eq%20%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27%20and%20Datastreams/properties/layerName%20eq%20%27Anzahl_Kfz_Zaehlstelle_15-Min%27&$count=true&$expand=Datastreams($filter=properties/layerName%20eq%20%27Anzahl_Kfz_Zaehlstelle_15-Min%27;$expand=Observations($top=10;$orderby=phenomenonTime%20desc))

- [Anzahl_Fahrraeder_Zaehlstelle_15-Min](https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName%20eq%20%27HH_STA_HamburgerRadzaehlnetz%27%20and%20Datastreams/properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlstelle_15-Min%27&$count=true&$expand=Datastreams($filter=properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlstelle_15-Min%27;$expand=Observations($top=10;$orderby=phenomenonTime%20desc)))

- [Verkehrslage](https://geodienste.hamburg.de/HH_WFS_Verkehrslage?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=de.hh.up:verkehrslage)
- [Luftmessnetz](https://geodienste.hamburg.de/HH_WFS_Luftmessnetz?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:luftmessnetz_messwerte)
- [Straßenwetterinformationssystems](https://geodienste.hamburg.de/DE_HH_INSPIRE_WFS_SWIS_Sensoren?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:swis_sensoren)
- [Stadtrad Stationen](https://geodienste.hamburg.de/HH_WFS_Stadtrad?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:stadtrad_stationen)
