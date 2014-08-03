package org.gazzax.labs.jena.nosql.cassandra;

/**
 * CQL tablenames.
 * 
 * @author Andrea Gazzarini
 * 
 * @since 1.0
 */
public interface Table {
	String S_POC = "s_poc";
	String O_SPC = "o_spc";
	String PO_SC = "po_sc";
	String PO_SC_INDEX_P = "po_sc_index_p";
	
	String OC_PS = "oc_ps";
	String C_OPS = "c_ops";
	String SC_OP = "sc_op";

	String SPC_O = "spc_o"; 
	String SPC_O_INDEX_PC = "spc_o_index_pc";		
}