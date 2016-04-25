from geopy.geocoders import Nominatim
from geopy.distance import great_circle
from geopy.distance import vincenty

#3550;495;50035;(AFOROS)OPORTO E-O(PORTALEGRE-ELVAS);438599,042006;4471016,470000
line = "3550;495;50035;(AFOROS)OPORTO E-O(PORTALEGRE-ELVAS);438599,042006;4471016,470000"
line = line.split(";")
id = line[0]
elementType = line[1]
codCent = line[2]
localizacion = line[3]
latitude = float(line[4].replace(",", "."))
longitude = float(line[5].replace(",", "."))

 # Urbanizacion Embajada Barajas -> Soto Hidalgo Madrid
location1 = Nominatim().geocode("Soto Hidalgo Madrid")
location2 = Nominatim().geocode("Farolillo Madrid")
location3 = Nominatim().geocode("Calle Alcala Madrid")
location4 = Nominatim().geocode("Plaza Fernandez Ladreda Madrid")

AIR_STATIONS = []
station = ["Soto Hidalgo Madrid", location1.latitude, location1.longitude];
AIR_STATIONS.append(station)

station = ["Farolillo Madrid", location2.latitude, location2.longitude];
AIR_STATIONS.append(station)

station = ["Alcala Madrid", location3.latitude, location3.longitude];
AIR_STATIONS.append(station)

station = ["Plaza Fernandez Ladreda Madrid", location4.latitude, location4.longitude];
AIR_STATIONS.append(station)

#print AIR_STATIONS
shortestDistance = 0.0;
for i in AIR_STATIONS:
    distance = great_circle({latitude, longitude}, {i[1], i[2]}).km
    print i[0] + "(" + str(i[1]) + "," + str(i[2]) + ") - " + str(distance)
    if (shortestDistance == 0.0):
        shortestDistance = distance;
    else:
        if (shortestDistance > distance):
            shortestDistance = distance

print "La distancia mas corta es: " + str(shortestDistance)
