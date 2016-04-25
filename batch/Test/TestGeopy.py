from geopy.geocoders import Nominatim

if __name__ == "__main__":
    #28079007 - Plaza Marques de Salamanca Madrid --> Plaza del Marques de Salamanca Madrid
    #28079018 - Calle Farolillo Madrid -> Farolillo Madrid
    # #28079023 - Alcala Madrid -> Alcala Madrid
    #28079026 - Urbanizacion Embajada Barajas Madrid --> Soto Hidalgo Madrid
    ## 28079099 - RED Madrid
    # 28079014 - Plaza Fernadez Ladreda --> Plaza Fernandez Ladreda


    # Urbanizacion Embajada Barajas -> Soto Hidalgo Madrid
    location = Nominatim().geocode("Soto Hidalgo Madrid")
    print(location.address)
    print(location.latitude)
    print(location.longitude)


