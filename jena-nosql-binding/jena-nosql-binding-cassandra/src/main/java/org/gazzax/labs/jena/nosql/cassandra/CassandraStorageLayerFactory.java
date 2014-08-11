package org.gazzax.labs.jena.nosql.cassandra;

import static org.gazzax.labs.jena.nosql.cassandra.Table.C_OPS;
import static org.gazzax.labs.jena.nosql.cassandra.Table.OC_PS;
import static org.gazzax.labs.jena.nosql.cassandra.Table.O_SPC;
import static org.gazzax.labs.jena.nosql.cassandra.Table.PO_SC;
import static org.gazzax.labs.jena.nosql.cassandra.Table.PO_SC_INDEX_P;
import static org.gazzax.labs.jena.nosql.cassandra.Table.SC_OP;
import static org.gazzax.labs.jena.nosql.cassandra.Table.SPC_O;
import static org.gazzax.labs.jena.nosql.cassandra.Table.SPC_O_INDEX_PC;
import static org.gazzax.labs.jena.nosql.cassandra.Table.S_POC;

import java.util.Map;

import org.gazzax.labs.jena.nosql.cassandra.dao.Cassandra2xBidirectionalMapDAO;
import org.gazzax.labs.jena.nosql.cassandra.dao.Cassandra2xMapDAO;
import org.gazzax.labs.jena.nosql.cassandra.dao.CassandraTripleIndexDAO;
import org.gazzax.labs.jena.nosql.cassandra.graph.CassandraGraph;
import org.gazzax.labs.jena.nosql.fwk.InitialisationException;
import org.gazzax.labs.jena.nosql.fwk.configuration.Configuration;
import org.gazzax.labs.jena.nosql.fwk.dictionary.TopLevelDictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.node.CacheNodectionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.node.KnownURIsDictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.node.PersistentNodeDictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.node.ThreeTieredNodeDictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.node.TransientNodeDictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.string.CacheStringDictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.string.PersistentStringDictionary;
import org.gazzax.labs.jena.nosql.fwk.dictionary.string.TransientStringDictionary;
import org.gazzax.labs.jena.nosql.fwk.ds.MapDAO;
import org.gazzax.labs.jena.nosql.fwk.ds.TripleIndexDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.hp.hpl.jena.graph.Graph;

/**
 * Concrete factory for creating Cassandra-backed domain and data access objects.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CassandraStorageLayerFactory extends StorageLayerFactory {

	private Session session;
	private TopLevelDictionary dictionary;
	
	@Override
	public <K, V> MapDAO<K, V> getMapDAO(
			final Class<K> keyClass, 
			final Class<V> valueClass, 
			final boolean isBidirectional, 
			final String name) {
		
		final CoDec<K> k = CoDec.get(keyClass);
		final CoDec<V> v = CoDec.get(valueClass);
		
		return isBidirectional
				? new Cassandra2xBidirectionalMapDAO<K, V>(
						session, 
						name, 
						k, 
						v)
				: new Cassandra2xMapDAO<K, V>(
						session, 
						name, 
						k, 
						v);
	}

	@Override
	public TripleIndexDAO getTripleIndexDAO() {
		return new CassandraTripleIndexDAO(session, dictionary);
	}

	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {
		final String hosts = configuration.getParameter("cassandra-contact-points", "localhost");

		final Cluster.Builder builder = Cluster.builder()
				.addContactPoints(hosts.split(","))
				.withLoadBalancingPolicy(configureLoadBalancingPolicy(configuration))
				.withQueryOptions(configureQueryOptions(configuration))
				.withPoolingOptions(configurePoolingOptions(configuration))
				.withReconnectionPolicy(configureReconnectionPolicy(configuration))
				.withRetryPolicy(configureRetryPolicy(configuration))
				.withSocketOptions(configureSocketOptions(configuration));

		final Compression compression = configureCompression(configuration);

		if (compression != null) {
			builder.withCompression(compression);
		}
		
		final String keyspaceName = configuration.getParameter("keyspace-name", "C2XDB");
		final Boolean createSchema = configuration.getParameter("create-schema", Boolean.TRUE);
				
		final Cluster cluster = builder.build();
		final Metadata metadata = cluster.getMetadata();
		final KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspaceName);

		session = builder.build().connect();

		if (keyspaceMetadata == null) {	
			createKeyspace(keyspaceName, configuration);
			session.execute("USE " + keyspaceName);
			if (createSchema) {
				createSchema(configuration);
			}
		} else {
			session.execute("USE " + keyspaceName);
		}
		
		dictionary = new CacheNodectionary(
						"TopLevelCacheDictionary",
						new KnownURIsDictionary(
							"KnownURIsDictionary",
							new ThreeTieredNodeDictionary(
									"ThreeTieredDictionary",
									new CacheStringDictionary(
											"NamespacesCacheDictionary",
											new PersistentStringDictionary("NamespacesDictionary", "DICT_NAMESPACES"),
											configuration.getParameter("namespaces-id-cache-size", Integer.valueOf(1000)),
											configuration.getParameter("namespaces-value-cache-size", Integer.valueOf(1000)),
											false),
									new TransientStringDictionary("LocalNamesDictionary"),
									new CacheNodectionary(
											"LiteralsAndBNodesCacheDictionary",
											new TransientNodeDictionary(
													"LiteralAndBNodesDictionary",
													new PersistentNodeDictionary("LongLiteralsDictionary"),
													configuration.getParameter("long-literals-threshold", Integer.valueOf(1000))),
											configuration.getParameter("literals-bnodes-id-cache-size", Integer.valueOf(50000)),
											configuration.getParameter("literals-bnodes-value-cache-size", Integer.valueOf(50000)),										
											true))),
					configuration.getParameter("known-uris-id-cache-size", Integer.valueOf(2000)),
					configuration.getParameter("known-uris-value-cache-size", Integer.valueOf(2000)),
					true);
		try {
			dictionary.initialise(this);
		} catch (InitialisationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TopLevelDictionary getDictionary() {
		return dictionary;
	}
	

	@Override
	public Graph getGraph() {
		return new CassandraGraph(this);
	}	
	
	@Override
	public String getInfo() {
		return "Cassandra binding v1.0";
	}			
	
	/**
	 * Creates necessary schema.
	 * 
	 * @param configuration the storage configuration.
	 */
	private void createSchema(final Configuration<Map<String, Object>> configuration) {
		final StringBuilder options = new StringBuilder()
				.append(" WITH compaction = {'class': '") 
				.append(configuration.getParameter("compaction-strategy", "SizeTieredCompactionStrategy")) 
				.append("'}")
				.append(" AND compression = {'sstable_compression' : '")
				.append(configuration.getParameter("compression-strategy", "LZ4Compressor")) 
				.append("'}")
				.append(" AND caching = '")
				.append(configuration.getParameter("caching-strategy", "keys_only")) 
				.append("'");
		
		session.execute("CREATE TABLE " + S_POC + "(s BLOB, p BLOB, o BLOB, c BLOB, PRIMARY KEY (s, p, o, c)) " + options);
		session.execute("CREATE TABLE " + O_SPC + "(o BLOB, s BLOB, p BLOB, c BLOB, PRIMARY KEY (o, s, p, c)) " + options);
		session.execute("CREATE TABLE " + PO_SC + "(p BLOB, o BLOB, s BLOB, c BLOB, p_index BLOB, PRIMARY KEY ((p, o), s, c)) " + options);
		session.execute("CREATE INDEX " + PO_SC_INDEX_P + " ON " + PO_SC + "(p_index)");
		
		session.execute("CREATE TABLE " + OC_PS + "(o BLOB, c BLOB, p BLOB, s BLOB, PRIMARY KEY ((o, c), p, s)) " + options);
		session.execute("CREATE TABLE " + C_OPS + "(c BLOB, o BLOB, p BLOB, s BLOB, PRIMARY KEY (c, o, p, s)) " + options);
		session.execute("CREATE TABLE " + SC_OP + "(s BLOB, c BLOB, o BLOB, p BLOB, PRIMARY KEY ((s, c), o, p)) " + options);

		session.execute("CREATE TABLE " + SPC_O + "(s BLOB, p BLOB, c BLOB, o BLOB, pc_index BLOB, PRIMARY KEY ((s, p, c), o)) " + options);
		session.execute("CREATE INDEX " + SPC_O_INDEX_PC + " ON " + SPC_O + "(pc_index)");						
	}
	
	void createKeyspace(final String keyspaceName, final Configuration<Map<String, Object>> configuration) {
		session.execute(
				new StringBuilder()
					.append("CREATE KEYSPACE ")
					.append(keyspaceName)
					.append(" WITH replication = {'class': '")
					.append(configuration.getParameter("replication-factor-strategy", "SimpleStrategy"))
					.append("', 'replication_factor': ")
					.append(configuration.getParameter("replication-factor", 1))
					.append("}")
					.toString());		
	}
	
	/**
	 * Returns the compression options according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the compression options according with a given configuration.
	 */
	private Compression configureCompression(final Configuration<Map<String, Object>> configuration) {
		Compression compression = null;
		try {
			final String compressionOption = configuration.getParameter("transport-compression", null);
			if (compressionOption != null) {
				compression = Compression.valueOf(compressionOption);
			}
		} catch (final Exception e) {
			// Ignore and don't set the compression.
		}
		return compression;
	}
	
	/**
	 * Returns the load balancing policy according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the load balancing policy according with a given configuration.
	 */
	private LoadBalancingPolicy configureLoadBalancingPolicy(final Configuration<Map<String, Object>> configuration) {
		LoadBalancingPolicy lbPolicy = Policies.defaultLoadBalancingPolicy();
		try {
			final String lbPolicyClassName = configuration.getParameter("load-balancing-policy-class-name", null);
			if (lbPolicyClassName != null) {
				lbPolicy = (LoadBalancingPolicy) Class.forName(lbPolicyClassName).newInstance();
			}
		} catch (final Exception ignore) {
			// just use the default value.
		}
		return lbPolicy;
	}

	/**
	 * Creates the pooling options for this factory.
	 * 
	 * @param configuration the configuration.
	 * @return the pooling options for this factory.
	 */
	private PoolingOptions configurePoolingOptions(final Configuration<Map<String, Object>> configuration) {
		final PoolingOptions poolingOptions = new PoolingOptions();

		Integer value = configuration.getParameter("local_core_connections_per_host", null);
		if (value != null) {
			poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, value);
		}

		value = configuration.getParameter("local_max_connections_per_host", null);
		if (value != null) {
			poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, value);
		}

		value = configuration.getParameter("local_max_simultaneous_request_per_connection_threshold", null);
		if (value != null) {
			poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, value);
		}

		value = configuration.getParameter("local_min_simultaneous_request_per_connection_threshold", null);
		if (value != null) {
			poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, value);
		}

		value = configuration.getParameter("remote_core_connections_per_host", null);
		if (value != null) {
			poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, value);
		}

		value = configuration.getParameter("remote_max_connections_per_host", null);
		if (value != null) {
			poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, value);
		}

		value = configuration.getParameter("remote_max_simultaneous_request_per_connection_threshold", null);
		if (value != null) {
			poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, value);
		}

		value = configuration.getParameter("remote_min_simultaneous_request_per_connection_threshold", null);
		if (value != null) {
			poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, value);
		}

		return poolingOptions;
	}

	/**
	 * Creates the query options for this factory.
	 * 
	 * @param configuration the configuration.
	 * @return the query options for this factory.
	 */
	private QueryOptions configureQueryOptions(final Configuration<Map<String, Object>> configuration) {
		final QueryOptions queryOptions = new QueryOptions();

		queryOptions.setConsistencyLevel(
					ConsistencyLevel.valueOf(
							configuration.getParameter(
									"consistency_level",
									"ONE")));

		queryOptions.setSerialConsistencyLevel(
					ConsistencyLevel.valueOf(
							configuration.getParameter(
									"serial_consistency_level",
									"ONE")));

		queryOptions.setFetchSize(configuration.getParameter(
						"fetch_size",
						1000));

		return queryOptions;
	}

	/**
	 * Returns the reconnection balancing policy according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the reconnection balancing policy according with a given configuration.
	 */
	private ReconnectionPolicy configureReconnectionPolicy(final Configuration<Map<String, Object>> configuration) {
		ReconnectionPolicy reconnectionPolicy = Policies.defaultReconnectionPolicy();
		try {
			final String reconnectionPolicyClassName = configuration.getParameter("reconnection_policy", null);
			if (reconnectionPolicyClassName != null) {
				reconnectionPolicy = (ReconnectionPolicy) Class.forName(reconnectionPolicyClassName).newInstance();
			}
		} catch (final Exception ignore) {
			// ignore and just use the default value.
		}
		return reconnectionPolicy;
	}

	/**
	 * Returns the retry balancing policy according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the retry balancing policy according with a given configuration.
	 */
	private RetryPolicy configureRetryPolicy(final Configuration<Map<String, Object>> configuration) {
		RetryPolicy retryPolicy = Policies.defaultRetryPolicy();
		try {
			final String retryPolicyClassName = configuration.getParameter("retry_policy", null);
			if (retryPolicyClassName != null) {
				retryPolicy = (RetryPolicy) Class.forName(retryPolicyClassName).newInstance();
			}
		} catch (final Exception ignore) {
			// just use the default value.
		}
		return retryPolicy;
	}

	/**
	 * Creates the socket options for this factory.
	 * 
	 * @param configuration the configuration.
	 * @return the socket options for this factory.
	 */
	private SocketOptions configureSocketOptions(final Configuration<Map<String, Object>> configuration) {
		final SocketOptions socketOptions = new SocketOptions();

		socketOptions.setConnectTimeoutMillis(
				configuration.getParameter(
						"connect_timeout_millis",
						SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS));

		socketOptions.setReadTimeoutMillis(
				configuration.getParameter(
						"read_timeout_millis",
						SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS));

		final Boolean keepAlive = configuration.getParameter("keep_alive", null);
		if (keepAlive != null) {
			socketOptions.setKeepAlive(keepAlive);
		}

		final Integer soLinger = configuration.getParameter("so_linger", null);
		if (soLinger != null) {
			socketOptions.setSoLinger(soLinger);
		}

		final Integer receiveBufferSize = configuration.getParameter("receive_buffer_size", null);
		if (receiveBufferSize != null) {
			socketOptions.setReceiveBufferSize(receiveBufferSize);
		}

		final Boolean tcpNoDelay = configuration.getParameter("tcp_no_delay", null);
		if (tcpNoDelay != null) {
			socketOptions.setTcpNoDelay(tcpNoDelay);
		}

		final Boolean reuseAddress = configuration.getParameter("reuse_address", null);
		if (reuseAddress != null) {
			socketOptions.setReuseAddress(reuseAddress);
		}

		return socketOptions;
	}
}
