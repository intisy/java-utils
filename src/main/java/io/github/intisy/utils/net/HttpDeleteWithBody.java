package io.github.intisy.utils.net;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import java.net.URI;

/**
 * A custom HTTP DELETE method implementation that supports request bodies.
 * The standard HTTP DELETE method in Apache HttpClient does not support request bodies,
 * but some REST APIs require DELETE requests with a body. This class extends
 * HttpEntityEnclosingRequestBase to allow for DELETE requests with a body.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {

    /**
     * The HTTP method name for DELETE requests.
     */
    public static final String METHOD_NAME = "DELETE";

    /**
     * Creates a new HttpDeleteWithBody instance.
     */
    public HttpDeleteWithBody() {
        super();
    }

    /**
     * Creates a new HttpDeleteWithBody instance with the specified URI.
     *
     * @param uri the URI to which the request will be sent
     */
    public HttpDeleteWithBody(final URI uri) {
        super();
        setURI(uri);
    }

    /**
     * Creates a new HttpDeleteWithBody instance with the specified URI string.
     * The string will be converted to a URI.
     *
     * @param uri the URI string to which the request will be sent
     */
    public HttpDeleteWithBody(final String uri) {
        super();
        setURI(URI.create(uri));
    }

    /**
     * Returns the HTTP method name, which is "DELETE" for this class.
     *
     * @return the HTTP method name
     */
    @Override
    public String getMethod() {
        return METHOD_NAME;
    }
}
