package web.servlet.matching.wikidata;

public class EndpointException extends Exception {
	  Endpoint _endpoint;
	  String _response;
	  Integer _httpcode;

	  public EndpointException(
	          Endpoint endpoint,
	          String message,
	          String response,
	          Integer httpcode
	          ) {
	    super(message);

	    _endpoint = endpoint;
	    _response = response;
	    _httpcode = httpcode;
	  }   

	  public String getMessage()
	  {
	    return 
	      "Error query  : " + _endpoint.getQuery() + "\n" +
	      "Error endpoint: " + _endpoint.getEndpoint() + "\n" +
	      "Error http_response_code: " + _httpcode + "\n" +
	      "Error message: " + _response + "\n" +
	      "Error data: \n" + super.getMessage() + "\n" ;
	  }
	}
