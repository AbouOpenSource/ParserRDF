package abouopensource;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Main {


    static String database="one";

    static String domain = "https://abouopensource.github.io#";
    static String dbpedia = "http://www.dbpedia.org/ontology#";


    public static void main(String[] args) throws IOException, JSONException {
        String datasetURL = "http://localhost:3030/"+database;
        String sparqlEndpoint = datasetURL + "/sparql";
        String sparqlUpdate = datasetURL + "/update";
        String graphStore = datasetURL + "/data";
        RDFConnection connection = RDFConnectionFactory.connect(sparqlEndpoint,sparqlUpdate,graphStore);
        Model model = ModelFactory.createDefaultModel();

        parseBikeStationStEtienne(model);
        parseBikeStationToulouse(model);
        parseTrainStationSaintEtienne(model,connection);
        parseTrainStationToulouse(model, connection);
        parseArretTransportSaintEtienne(model, connection);
        parseArretTransportToulouse(model, connection);
        parseSchoolSaintEtienne(model, connection);
        parseSchoolToulouse(model,connection);
        parseZoneChargeToulouse(model,connection);
        connection.load(model);
        connection.close();
    }

    static public void parseBikeStationStEtienne(Model model) throws IOException, JSONException {
        Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
        Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
        Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
        Property hasRealTime = model.createProperty(domain+"hasRealTime");
        Property method = model.createProperty(domain+"method");
        Property localId = model.createProperty(domain+"local_id");
        Property pathRealTimeData = model.createProperty(domain+"pathRealTimeData");
        Resource st_etienne = model.createResource("http://www.dbpedia.org/resource/Saint-Étienne");
        Resource bike_system= model.createResource("http://www.dbpedia.org/resource/Bicycle-sharing_system");
        Property dataFormat= model.createProperty("https://www.wikidata.org/wiki/Q494823");
        Resource jsonRessource = model.createResource("https://www.wikidata.org/wiki/Q2063");
        Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
        String content = new String(Files.readAllBytes(Paths.get("data/source/Bike/saint-etienne.json")));
        JSONObject objet = new JSONObject(content);
        JSONArray array = objet.getJSONObject("data").getJSONArray("stations");
        for (int i = 0; i < array.length(); i++) {
            Resource subject = model.createResource(domain+"BS"+array.getJSONObject(i).get("station_id").toString()+"ST-ETIENNE");
            model.add(subject,RDF.type,spatialThing);
            model.add(subject,city,st_etienne);
            model.add(subject, RDF.type, bike_system);
            model.add(subject, RDFS.label,model.createLiteral(array.getJSONObject(i).get("name").toString()));
            model.add(subject, property_lat, model.createTypedLiteral(Float.parseFloat(array.getJSONObject(i).get("lat").toString())));
            model.add(subject, property_long, model.createTypedLiteral(Float.parseFloat(array.getJSONObject(i).get("lon").toString())));
            model.add(subject,hasRealTime,model.createTypedLiteral("https://saint-etienne-gbfs.klervi.net/gbfs/en/station_status.json"));
            model.add(subject,dataFormat,jsonRessource);
            model.add(subject, localId,model.createTypedLiteral(Integer.parseInt(array.getJSONObject(i).get("station_id").toString())));
            model.add(subject,dataFormat,jsonRessource);
            model.add(subject,pathRealTimeData,model.createTypedLiteral("/data/stations"));
        }
        System.out.println("parseBikeStationStEtienne done");
    }

    static public void parseTrainStationSaintEtienne(Model model,RDFConnection connection){

        try (Stream<String> stream = Files.lines(Paths.get("data/source/TER/saint-etienne.txt")).skip(1)) {
            Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
            Resource st_etienne = model.createResource("http://www.dbpedia.org/resource/Saint-Étienne");

            Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
            Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
            Resource train_station = model.createResource("http://www.dbpedia.org/resource/Train_station");
            stream.forEach((String line)->{
                String[] items = line.split(",");
                Resource subject = model.createResource(domain+items[0].toString());
                model.add(subject, RDF.type,spatialThing);
                model.add(subject,city,st_etienne);
                model.add(subject, RDF.type, train_station);
                model.add(subject, RDFS.label, model.createLiteral(items[1].toString(),"fr"));
                model.add(subject, property_lat, model.createTypedLiteral(Float.valueOf(items[3].toString())));
                model.add(subject,property_long, model.createTypedLiteral(Float.valueOf(items[4].toString())));
              //  connection.load(model);
            });
            System.out.println("parseTrainStationStEtienne  done");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }


    }

    static public void parseTrainStationToulouse(Model model,RDFConnection connection){

        try (Stream<String> stream = Files.lines(Paths.get("data/source/TER/toulouse.txt")).skip(1)) {
            Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
            Resource toulouse = model.createResource("http://www.dbpedia.org/resource/Toulouse");

            Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
            Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
            Resource train_station = model.createResource("http://www.dbpedia.org/resource/Train_station");
            stream.forEach((String line)->{
                String[] items = line.split(",");
                Resource subject = model.createResource(domain+items[0].toString());
                model.add(subject, RDF.type,spatialThing);
                model.add(subject, city, toulouse);
                model.add(subject, RDF.type,train_station);
                model.add(subject, RDFS.label, model.createLiteral(items[1].toString(),"fr"));
                model.add(subject, property_lat, model.createTypedLiteral(Float.valueOf(items[3].toString())));
                model.add(subject,property_long, model.createTypedLiteral(Float.valueOf(items[4].toString())));
                //connection.load(model);
            });
            System.out.println("parseTrainStationToulouse done");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }


    }

    static public void parseBikeStationToulouse(Model model) throws IOException, JSONException {
        Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
        Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
        Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
        Property hasRealTime = model.createProperty(domain+"hasRealTime");
        Property localId = model.createProperty(domain+"local_id");
        Property pathRealTimeData = model.createProperty(domain+"pathRealTimeData");
        Resource toulouse = model.createResource("http://www.dbpedia.org/resource/Toulouse");
        Resource bike_system= model.createResource("http://www.dbpedia.org/resource/Bicycle-sharing_system");
        Property dataFormat= model.createProperty("https://www.wikidata.org/wiki/Q494823");
        Resource jsonResource = model.createResource("https://www.wikidata.org/wiki/Q2063");
        Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
        try (Stream<String> stream = Files.lines(Paths.get("data/source/Bike/velo-toulouse.csv")).skip(1)) {
            Resource object = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            stream.forEach((String line)->{
                String[] items = line.split(";");
                Resource subject = model.createResource(domain+items[3]+"TOULOUSE");
                model.add(subject,RDF.type,spatialThing);
                model.add(subject,city,toulouse);
                model.add(subject, RDF.type, bike_system);
                model.add(subject, RDFS.label,model.createLiteral(items[2]));
                model.add(subject, property_lat, model.createTypedLiteral(Float.parseFloat(items[0].split(",")[0])));
                model.add(subject, property_long, model.createTypedLiteral(Float.parseFloat(items[0].split(",")[1])));
                model.add(subject,hasRealTime,model.createTypedLiteral("https://transport.data.gouv.fr/gbfs/toulouse/station_status.json"));
                model.add(subject,dataFormat,jsonResource);
                model.add(subject, localId,model.createTypedLiteral(Integer.parseInt(items[3])));
                model.add(subject,dataFormat,jsonResource);
                model.add(subject,pathRealTimeData,model.createTypedLiteral("/data/stations"));
            });
            System.out.println("parseBikeStationToulouse done");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static public void parseArretTransportSaintEtienne(Model model, RDFConnection connection){
        try (Stream<String> stream = Files.lines(Paths.get("data/source/arrets/station-saint-etienne.txt")).skip(1)) {
            Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            Resource bus_stop = model.createResource("http://www.dbpedia.org/resource/Bus_stop");
            Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
            Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
            Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
            Resource st_etienne = model.createResource("http://www.dbpedia.org/resource/Saint-Étienne");


            stream.forEach((String line)->{
                String[] items = line.split(",");
                Resource subject = model.createResource(domain+"SSTAS:"+items[0].toString());
                model.add(subject, RDF.type,spatialThing);
                model.add(subject, RDF.type,bus_stop);
                model.add(subject,city,st_etienne);
                model.add(subject, RDFS.label, model.createLiteral(items[1].toString(),"fr"));
                model.add(subject, property_lat, model.createTypedLiteral(Float.valueOf(items[2].toString())));
                model.add(subject,property_long, model.createTypedLiteral(Float.valueOf(items[3].toString())));
               // connection.load(model);
            });
            System.out.println("parseArretTransportSaintEtienne done");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    static public void parseArretTransportToulouse(Model model, RDFConnection connection){
        try (Stream<String> stream = Files.lines(Paths.get("data/source/arrets/stations-de-toulous.csv")).skip(1)) {
            Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            Resource bus_stop = model.createResource("http://www.dbpedia.org/resource/Bus_stop");
            Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
            Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
            Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
            Resource toulouse = model.createResource("http://www.dbpedia.org/resource/Toulouse");

            stream.forEach((String line)->{
                String[] items = line.split(";");
                Resource subject = model.createResource(domain+"ArretTL:"+items[3].replaceAll("\\s+",""));
                model.add(subject, RDF.type, spatialThing);
                model.add(subject, RDF.type, bus_stop);
                model.add(subject, city, toulouse);
                model.add(subject, RDFS.label, model.createLiteral(items[3].toString(),"fr"));
                model.add(subject, property_lat, model.createTypedLiteral(Float.valueOf(items[0].split(",")[0])));
                model.add(subject,property_long, model.createTypedLiteral(Float.valueOf(items[0].split(",")[1])));
               // connection.load(model);
            });
            System.out.println("parseTrainStation done");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    static public void parseZoneChargeToulouse(Model model, RDFConnection connection){
        try (Stream<String> stream = Files.lines(Paths.get("data/source/placederecharche/bornes-recharge-electrique.csv")).skip(1)) {
            Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            Resource bus_stop = model.createResource("http://www.dbpedia.org/resource/Bus_stop");
            Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
            Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");

            stream.forEach((String line)->{
                String[] items = line.split(";");
                Resource subject = model.createResource(domain+"ArretTL:"+items[0].toString());
                model.add(subject, RDF.type,spatialThing);
                model.add(subject, RDF.type,bus_stop);
                model.add(subject, RDFS.label, model.createLiteral(items[3].toString(),"fr"));
                model.add(subject, property_lat, model.createTypedLiteral(Float.valueOf(items[0].split(",")[0])));
                model.add(subject,property_long, model.createTypedLiteral(Float.valueOf(items[0].split(",")[1])));
               // connection.load(model);
            });
            System.out.println("parseTrainStation done");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    static public void parseSchoolSaintEtienne(Model model, RDFConnection connection){
        Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
        Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
        Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
        Resource st_etienne = model.createResource("http://www.dbpedia.org/resource/Saint-Étienne");
        Resource school = model.createResource("http://www.dbpedia.org/resource/School");

        Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");

        try (Stream<String> stream = Files.lines(Paths.get("data/source/school/st-etienne-school.csv")).skip(1)) {
            Resource object = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            stream.forEach((String line)->{
                String[] items = line.split(";");
                Resource subject = model.createResource(domain+items[0]+"ST_ET");
                model.add(subject, RDF.type, school);
                model.add(subject, RDFS.label, model.createLiteral(items[1], "fr"));
                model.add(subject, RDF.type, spatialThing);

                model.add(subject, city, st_etienne);
                model.add(subject, property_lat, model.createTypedLiteral(Float.parseFloat(items[14])));
                model.add(subject, property_long, model.createTypedLiteral(Float.parseFloat(items[15])));
               // connection.load(model);
            });
            System.out.println("parseSchoolStEtienne done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static public void parseSchoolToulouse(Model model, RDFConnection connection){
        Property property_lat = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat");
        Property property_long = model.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long");
        Property city = model.createProperty("http://www.dbpedia.org/ontology/city");
        Resource toulouse = model.createResource("http://www.dbpedia.org/resource/Toulouse");
        Resource school = model.createResource("http://www.dbpedia.org/resurce/School");
        Resource spatialThing = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");

        try (Stream<String> stream = Files.lines(Paths.get("data/source/school/toulouse-school.csv")).skip(1)) {
            Resource object = model.createResource("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing");
            stream.forEach((String line)->{
                String[] items = line.split(";");
                Resource subject = model.createResource(domain+items[2]+"TLSchool");
                model.add(subject, RDF.type, school);
                model.add(subject, RDFS.label, model.createLiteral(items[3], "fr"));
                model.add(subject, city, toulouse);
                model.add(subject,RDF.type,spatialThing);
                model.add(subject, property_lat, model.createTypedLiteral(Float.parseFloat(items[0].split(",")[0])));
                model.add(subject, property_long, model.createTypedLiteral(Float.parseFloat(items[0].split(",")[0])));
                //connection.load(model);
            });
            System.out.println("parseSchoolToulouse done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
