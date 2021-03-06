package web.servlet.matching.wikidata;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author Karima Rafes.
 */


public class Endpoint {

  /**
   * Root of the URL Endpoint
   * @access private
   * @var string
   */
  private String _endpoint_root;

  /**
   * URL of Endpoint to read
   * @access private
   * @var string
   */
  private String _endpoint;

  /**
   * URL  sparql to write
   * @access private
   * @var string
   */
  private String _endpoint_write;

  /**
   * in the constructor set debug to true in order to get usefull output
   * @access private
   * @var bool
   */
  private boolean _debug;

  /**
   * last SPARQL query
   * @access private
   * @var String
   */
  private String _query;

  /**
   * in the constructor set the right to write or not in the store
   * @access private
   * @var bool
   */
  private boolean _readOnly;

  /**
   * in the constructor set the proxy_host if necessary
   * @access private
   * @var string
   */
  private String _proxy_host;

  /**
   * in the constructor set the proxy_port if necessary
   * @access private
   * @var int
   */
  private int _proxy_port;

  /**
   * Parser of XML result
   * @access private
   * @var ParserSparqlResult
   */
  private String _parserSparqlResult;

  /**
   * Name of parameter HTTP to send a query SPARQL to read data.
   * @access private
   * @var string
   */
  private String _nameParameterQueryRead;

  /**
   * Name of parameter HTTP to send a query SPARQL to write data.
   * @access private
   * @var string
   */
  private String _nameParameterQueryWrite;

  /**
   * Method HTTP to send a query SPARQL to read data.
   * @access private
   * @var string
   */
  private String _MethodHTTPRead;
  private String _MethodHTTPWrite;

  private String _login;
  private String _password;

  private SAXParser _parser;  
  private DefaultHandler _handler;
  private String _response;

  private String _userAgent = "BorderCloud/SPARQL-JAVA 1.0";

  public Endpoint(
    String endpoint
    ) {
    //Default value
    boolean readOnly = true;
    boolean debug = false;
    String proxy_host = null;
    Integer proxy_port = 0;

    init( endpoint, readOnly, debug, proxy_host, proxy_port);
  }

  public Endpoint( 
    String endpoint, 
    boolean readOnly
    ) {
    //Default value
    boolean debug = false;
    String proxy_host = null;
    Integer proxy_port = 0;

    init(endpoint, readOnly, debug, proxy_host, proxy_port);
  }
  public Endpoint(
    String endpoint, 
    Boolean readOnly, 
    Boolean debug
    ) {
    //Default value
    String proxy_host = null;
    Integer proxy_port = 0;

    init(endpoint, readOnly, debug, proxy_host, proxy_port);

    if(_debug) {
      java.util.logging.Logger.getLogger("org.apache.http.wire").setLevel(java.util.logging.Level.FINEST);
      java.util.logging.Logger.getLogger("org.apache.http.headers").setLevel(java.util.logging.Level.FINEST);

      System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
      System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
      System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire", "debug");
      System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "debug");
      System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.headers", "debug");
    }
  }

  public Endpoint(
    String endpoint, 
    Boolean readOnly, 
    Boolean debug, 
    String proxy_host, //todo
    Integer proxy_port//todo
    ) {

    init( endpoint, readOnly, debug, proxy_host, proxy_port);
  }

  private void init(
    String endpoint, 
    Boolean readOnly, 
    Boolean debug, 
    String proxy_host, //todo
    Integer proxy_port//todo
    ) {
    try {
      URL url = new URL(endpoint);

      if (readOnly) {
        _endpoint = endpoint;
      } else {
        _endpoint = endpoint;
        _endpoint_root = url.getProtocol() + "://"+url.getHost()+url.getFile();
      }
    }
    catch (MalformedURLException mue)
    {
      System.out.println("Ouch - a MalformedURLException happened.");
      mue.printStackTrace();
    }

    // Init Sax class
    SAXParserFactory parserSPARQL = SAXParserFactory.newInstance();
    _parser = null;

    try {
      _parser = parserSPARQL.newSAXParser();
    } 
    catch (ParserConfigurationException e) {
      e.printStackTrace();
    } 
    catch (SAXException e) {
      e.printStackTrace();
    }

    _debug = debug;
    _endpoint_write = ""; 
    _readOnly = readOnly;

    _proxy_host = proxy_host;
    _proxy_port = proxy_port;    

    if (_proxy_host != null && _proxy_port != 0) {
      //todo
    } else {
      //todo
    }

    // init parameter in the standard
    _nameParameterQueryRead = "query";
    _nameParameterQueryWrite = "update";    

    //FIX for Wikidata
    if ( endpoint == "https://query.wikidata.org/sparql") {
      _MethodHTTPRead= "GET";
    } else {
      _MethodHTTPRead= "POST"; // by default
    }
  }

  /**
   * Set the server password
   * @param password : server password
   * @access public
   */
  public void setPassword(String password) {
    _password = password;
  }

  /**
   * Get the server login
   * @return string $password : server password
   * @access public
   */
  public String  getPassword() {
    return _password;
  }

  /**
   * Set the server login
   * @param login : server login
   * @access public
   */
  public void setLogin(String login) {
    _login = login;
  }

  /**
   * Get the server login
   * @return string $login : server login
   * @access public
   */
  public String  getLogin() {
    return _login;
  }

  public String  getQuery() {
    return _query;
  }
  public String  getEndpoint() {
    return _endpoint;
  }

  public String  getResponse() {
    return _response;
  }

  /**
   * Set the User-Agent request header
   */
  public void setUserAgentRequestHeader(String userAgent) {
    _userAgent = userAgent;
  }

  public HashMap<String, HashMap> query(String query) 
    throws EndpointException  {
    _query = query;
    _handler = null;
    _response = null;
    String param = _nameParameterQueryRead;
    if (query.indexOf("INSERT") > -1 || query.indexOf("insert")> -1 ||
      query.indexOf("DELETE") > -1 || query.indexOf("delete")> -1 ||
      query.indexOf("CLEAR") > -1 || query.indexOf("clear")> -1 ) {
      param = _nameParameterQueryWrite;
    }

    if (_MethodHTTPRead.equalsIgnoreCase("POST")) {
      if (_login != null && _password != null) {
        return sendQueryPOSTwithAuth(_endpoint, param, query, _login, _password);
      } else {
        return sendQueryPOST(_endpoint, param, query);
      }
    } else {
      return sendQueryGET(_endpoint, param, query);
    }
  }

  private  HashMap<String, HashMap> getResult() throws EndpointException {
    //parse the message
    _handler = new ParserSPARQLResultHandler();

    try {
      _parser.parse(new InputSource(new StringReader(_response)), _handler);
    } 
    catch (SAXException e) {
//        System.out.println(e.getMessage());
//        e.printStackTrace();
      throw new EndpointException(
              this,
              e.getMessage(),
              _response,
              200);
    } 
    catch (IOException e) {
//        System.out.println(e.getMessage());
//        e.printStackTrace();
      throw new EndpointException(
              this,
              e.getMessage(),
              _response,
              200);
    }

    if (_handler != null) {
      return ((ParserSPARQLResultHandler) _handler).getResult();//new HashMap<String, HashMap>();
    } else {
      return null;
    }
  }

  /**
   * Set the method HTTP to read
   * @param method : HTTP method (GET or POST) for reading data (by default is POST)
   * @access public
   */
  public void setMethodHTTPRead(String method) {
    _MethodHTTPRead = method;
  }

  /**
   * Set the method HTTP to write
   * @param method : HTTP method (GET or POST) for writing data (by default is POST)
   * @access public
   */
  public void setMethodHTTPWrite(String method) {
    _MethodHTTPWrite = method;
  }

  private HashMap<String, HashMap> sendQueryGET(String urlStr, String parameter, String query) 
    throws EndpointException { 

    int statusCode=0;
    try {
      String url = urlStr + "?"+ parameter + "=" + URLEncoder.encode(query, "UTF-8"); 
      CloseableHttpClient httpclient = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom()
        // Waiting for a connection from connection manager
        .setConnectionRequestTimeout(10000)
        // Waiting for connection to establish
        .setConnectTimeout(5000)
        .setExpectContinueEnabled(false)
        // Waiting for data
        .setSocketTimeout(5000)
        .setCookieSpec("easy")
        .build())
        .setMaxConnPerRoute(20)
        .setMaxConnTotal(100)
        .build();

      try {
        HttpGet httpget = new HttpGet(url);

        httpget.setHeader("Accept", "application/sparql-results+xml");
        httpget.setHeader("Accept-Charset", "UTF-8");
        httpget.setHeader("User-Agent", _userAgent);

        //System.out.println("Executing request " + httpget.getRequestLine());
        CloseableHttpResponse response = httpclient.execute(httpget);
        try {
          statusCode = response.getStatusLine().getStatusCode();
          HttpEntity entity = response.getEntity();

          //System.out.println("----------------------------------------");
          //System.out.println(response.getStatusLine());
          _response = EntityUtils.toString(entity,"UTF-8");
          //EntityUtils.consume(entity);

          if ( statusCode < 200 || statusCode >= 300) {
            throw new EndpointException(this,
                    response.getStatusLine().toString(),
                    _response,
                    statusCode
                    );
          }
        } 
        finally {
          response.close();
        }
      } 
      finally {
        httpclient.close();
      }
    } 
    catch (Exception e) {
//        System.out.println(e.getMessage());
//        e.printStackTrace();
      throw new EndpointException(
              this,
              e.getMessage(),
              "",
              0);
    }
    return getResult();
  }




  private HashMap<String, HashMap> sendQueryPOSTwithAuth(
    String urlStr, String parameter, String query, 
    String login, String password) 
    throws EndpointException 
    { 

    int statusCode=0;
    try {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(
        new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), 
        new UsernamePasswordCredentials(login, password));
      CloseableHttpClient httpclient = HttpClients.custom()
        .setDefaultCredentialsProvider(credsProvider)
        .build();
      try {
        HttpPost httpPost = new HttpPost(urlStr);
        List <NameValuePair> nvps = new ArrayList <NameValuePair>();
        nvps.add(new BasicNameValuePair(parameter, query));
        httpPost.setEntity(new UrlEncodedFormEntity(nvps));
        CloseableHttpResponse response2 = httpclient.execute(httpPost);

        try {
          //System.out.println(response2.getStatusLine());
          statusCode = response2.getStatusLine().getStatusCode() ;
          HttpEntity entity2 = response2.getEntity();
          // do something useful with the response body
          // and ensure it is fully consumed
          ////System.out.println(EntityUtils.toString(entity2));

          _response = EntityUtils.toString(entity2);
          //EntityUtils.consume(entity2);

          if ( statusCode < 200 || statusCode >= 300) {
            throw new EndpointException(this,
                    response2.getStatusLine().toString(),
                    _response,
                    statusCode
            );
          }
        } 
        finally {
          response2.close();
        }
      } 
      finally {
        httpclient.close();
      }
    } 
    catch (Exception e) {
//        System.out.println(e.getMessage());
//        e.printStackTrace();
      throw new EndpointException(
              this,
              e.getMessage(),
              "",
              0);
    }

    return getResult();
  }

  private HashMap<String, HashMap> sendQueryPOST(String urlStr, String parameter, String query) 
    throws EndpointException 
  { 
    //URL url = null;
    //int port = 0;
    int statusCode=0;
    try {
      // url = new URL(urlStr);

      //_endpointHost = url.getHost();
      //port = url.getPort() != -1 ? url.getPort() : url.getDefaultPort() ;
      // println(_endpointPort);
      // _clientHTTP = new Client(parent, _endpointHost, _endpointPort);

      CloseableHttpClient httpclient = HttpClients.custom()
        .build();
      try {
        HttpPost httpPost = new HttpPost(urlStr);
        httpPost.setHeader("Accept", "application/sparql-results+xml");
        httpPost.setHeader("Accept-Charset", "UTF-8");
        httpPost.setHeader("User-Agent", _userAgent);
        List <NameValuePair> nvps = new ArrayList <NameValuePair>();
        nvps.add(new BasicNameValuePair(parameter, query));
        httpPost.setEntity(new UrlEncodedFormEntity(nvps,"UTF-8"));
        CloseableHttpResponse response2 = httpclient.execute(httpPost);

        try {
          //System.out.println(response2.getStatusLine());
          statusCode = response2.getStatusLine().getStatusCode() ;

          HttpEntity entity2 = response2.getEntity();
          // do something useful with the response body
          // and ensure it is fully consumed
          //System.out.println(EntityUtils.toString(entity2));
          _response = EntityUtils.toString(entity2);
          //EntityUtils.consume(entity2);

          if ( statusCode < 200 || statusCode >= 300) {
            throw new EndpointException(this,
                    response2.getStatusLine().toString(),
                    _response,
                    statusCode
            );
          }
        } 
        finally {
          response2.close();
        }
      } 
      finally {
        httpclient.close();
      }
    }
    catch (Exception e) {
//        System.out.println(e.getMessage());
//        e.printStackTrace();
      throw new EndpointException(
              this,
              e.getMessage(),
              "",
              0);
    }

    return getResult();
  }
}