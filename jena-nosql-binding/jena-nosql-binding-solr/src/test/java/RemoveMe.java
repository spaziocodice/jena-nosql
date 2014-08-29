import static org.gazzax.labs.jena.nosql.fwk.TestUtility.DUMMY_BASE_URI;

import java.io.File;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;


public class RemoveMe {
	public static void main(String[] args) {
		StorageLayerFactory factory = StorageLayerFactory.getFactory();
		Dataset dataset = DatasetFactory.create(factory.getDatasetGraph());
		
		RDFDataMgr.read(
				factory.getDatasetGraph(), 
				new File("/work/data/jena-nosql/triples_gridpedia.nt").toURI().toString(), 
				DUMMY_BASE_URI,
				Lang.NTRIPLES);
//		final Model model = dataset.getDefaultModel().read(new File("/work/data/jena-nosql/triples_gridpedia.nt").toURI().toString(), DUMMY_BASE_URI, "N-TRIPLE");
		
//		System.out.println(model.size());
		System.out.println("RemoveMe.main()");
		factory.getClientShutdownHook().close();
	}
}
