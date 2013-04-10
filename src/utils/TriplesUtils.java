package utils;

import java.util.HashSet;
import java.util.Set;


public class TriplesUtils {
	public static final String S_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public static final String S_RDFS_SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
	public static final String S_RDF_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs";
	public static final String TCM_TREATMENT = "http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/treatment";
	public static final String TCM_ASSOCIATION = "http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/association";
	public static final String TCM_ASSO_TREAT = "http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/asso_treat";

	public static Set<String> subjectKeyPredicateSet = new HashSet<String>();
	public static Set<String> objectKeyPredicateSet = new HashSet<String>();
	
	static{
		subjectKeyPredicateSet.add(TCM_TREATMENT);
		subjectKeyPredicateSet.add(S_RDF_SAMEAS);
		subjectKeyPredicateSet.add(S_RDFS_SUBCLASS);
		
		objectKeyPredicateSet.add(TCM_ASSOCIATION);
		objectKeyPredicateSet.add(TCM_ASSO_TREAT);
		objectKeyPredicateSet.add(S_RDF_TYPE);
	}
}
