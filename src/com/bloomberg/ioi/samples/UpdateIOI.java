/* Copyright 2017. Bloomberg Finance L.P.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:  The above
 * copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

package com.bloomberg.ioi.samples;

import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Identity;
import java.io.IOException;
import java.time.Instant;

public class UpdateIOI {

    private static final Name SLOW_CONSUMER_WARNING = new Name("SlowConsumerWarning");
    private static final Name SLOW_CONSUMER_WARNING_CLEARED = new Name("SlowConsumerWarningCleared");
    private static final Name SESSION_STARTED = new Name("SessionStarted");
    private static final Name SESSION_STARTUP_FAILURE = new Name("SessionStartupFailure");
    private static final Name SERVICE_OPENED = new Name("ServiceOpened");
    private static final Name SERVICE_OPEN_FAILURE = new Name("ServiceOpenFailure");
    private static final Name AUTHORIZATION_SUCCESS = new Name("AuthorizationSuccess");
    private static final Name AUTHORIZATION_FAILURE = new Name("AuthorizationFailure");
    private static final Name HANDLE = new Name("handle");
    
    private String d_emsx;
    private String d_auth;
    private String d_host;
    private int d_port;
    private String d_user;
    private String d_ip;

    private static boolean quit = false;

    private CorrelationID requestID;

    public static void main(String[] args) throws java.lang.Exception
    {
        System.out.println("Bloomberg - IOI API Example - UpdateIOI\n");

        UpdateIOI example = new UpdateIOI();
        example.run(args);

        while (!quit) { };

        System.out.println("Press Any Key...");
        System.in.read();
    }

    public UpdateIOI()
    {

        // Define the service required, in this case the beta service, 
        // and the values to be used by the SessionOptions object
        // to identify IP/port of the back-end process.

        d_emsx = "//blp/ioiapi-beta-request";
        d_auth = "//blp/apiauth";
        d_host = "mbp-nj-dev.bdns.bloomberg.com";
        d_port = 8194;
        d_user = "CORP\\rclegg2";
        d_ip = "10.136.8.125";
    }

    private void run(String[] args) throws IOException
    {

        SessionOptions d_sessionOptions = new SessionOptions();
        d_sessionOptions.setServerHost(d_host);
        d_sessionOptions.setServerPort(d_port);

        Session session = new Session(d_sessionOptions, new EMSXEventHandler());

        session.startAsync();

    }

    class EMSXEventHandler implements EventHandler 
    {
    	
        private Identity identity;

	    public void processEvent(Event evt, Session session)
	    {
	        try
	        {
	            switch (evt.eventType().intValue())
	            {
	                case Event.EventType.Constants.ADMIN:
	                    processAdminEvent(evt, session);
	                    break;
	                case Event.EventType.Constants.SESSION_STATUS:
	                    processSessionEvent(evt, session);
	                    break;
	                case Event.EventType.Constants.SERVICE_STATUS:
	                    processServiceEvent(evt, session);
	                    break;
	                case Event.EventType.Constants.AUTHORIZATION_STATUS:
	                    processAuthorizationStatusEvent(evt, session);
	                    break;
	                case Event.EventType.Constants.RESPONSE:
	                    processResponseEvent(evt, session);
	                    break;
	                default:
	                    processMiscEvents(evt, session);
	                    break;
	            }
	        }
	        catch (Exception e)
	        {
	        	System.err.println(e);
	        }
	    }
	
	    private void processAdminEvent(Event evt, Session session)
	    {
	    	System.out.println("Processing " + evt.eventType().toString());
	
			MessageIterator msgIter = evt.messageIterator();

			while (msgIter.hasNext()) {
	            
				Message msg = msgIter.next();

				if (msg.messageType().equals(SLOW_CONSUMER_WARNING))
	            {
	            	System.err.println("Warning: Entered Slow Consumer status");
	            }
	            else if (msg.messageType().equals(SLOW_CONSUMER_WARNING_CLEARED))
	            {
	            	System.out.println("Slow consumer status cleared");
	            }
	        }
	    }
	
	
	    private void processSessionEvent(Event evt, Session session) throws IOException
	    {
	    	System.out.println("\nProcessing " + evt.eventType().toString());
	
			MessageIterator msgIter = evt.messageIterator();

			while (msgIter.hasNext()) {
	            
				Message msg = msgIter.next();
				
				System.out.println(msg);
				
				if (msg.messageType().equals(SESSION_STARTED))
	            {
	            	System.out.println("Session started...");
	                session.openServiceAsync(d_auth);
	            }
	            else if (msg.messageType().equals(SESSION_STARTUP_FAILURE))
	            {
	            	System.err.println("Error: Session startup failed");
	            }
	        }
	    }
	
	    private void processServiceEvent(Event evt, Session session) throws IOException
	    {
	
	    	System.out.println("\nProcessing " + evt.eventType().toString());
	
			MessageIterator msgIter = evt.messageIterator();

			while (msgIter.hasNext()) {
	            
				Message msg = msgIter.next();

				if (msg.messageType().equals(SERVICE_OPENED))
	            {
	                String serviceName = msg.asElement().getElementAsString("serviceName");
	
	                System.out.println("Service opened [" + serviceName + "]...");
	
	                if (serviceName == d_auth)
	                {
	                	System.out.println("Auth Service opened... Opening EMSX service...");
	                    session.openServiceAsync(d_emsx);
	                }
	                else if (serviceName == d_emsx)
	                {
	                	System.out.println("EMSX Service opened... Sending Authorization requests");
	
	                    sendAuthRequest(session, d_user, d_ip);
	
	                }
	            }
	            else if (msg.messageType().equals(SERVICE_OPEN_FAILURE))
	            {
	            	System.err.println("Error: Service failed to open");
	            }
	        }
	    }
	
	    private void processAuthorizationStatusEvent(Event evt, Session session)
	    {
	    	System.out.println("\nProcessing " + evt.eventType().toString());
	
			MessageIterator msgIter = evt.messageIterator();

			while (msgIter.hasNext()) {
	            
				Message msg = msgIter.next();

				System.out.println("AUTHORIZATION_STATUS message: " + msg.toString());
	        }
	    }
	
	    private void processResponseEvent(Event evt, Session session)
	    {
	    	System.out.println("Received Event: " + evt.eventType().toString());
	
			MessageIterator msgIter = evt.messageIterator();

			while (msgIter.hasNext()) {
	            
				Message msg = msgIter.next();

				if (msg.messageType().equals(AUTHORIZATION_SUCCESS))
	            {
	            	System.out.println("Authorization successfull...");
	
	                sendCreateIOI(session, d_emsx);
	            }
	            else if (msg.messageType().equals(AUTHORIZATION_FAILURE))
	            {
	            	System.out.println("Authorisation failed...");
	            	System.out.println(msg.toString());
	
	                // Here you can insert code to automatically retry the authorisation if required
	            }
	            else if (msg.correlationID()==requestID)
	            {
	            	System.out.println(msg.messageType() + ">>" + msg.toString());
	                
	                if (msg.messageType().equals(HANDLE))
	                {
	                    String val = msg.getElementAsString("value");
	                    System.out.println("Response: Value=" + val);
	                }
	                else
	                {
	                	System.out.println("Unexpected message...");
	                	System.out.println(msg.toString());
	                }
	
	                quit = true;
	            }
	            else
	            {
	            	System.out.println("Unexpected authorization message...");
	            	System.out.println(msg.toString());
	            }
	        }
	    }
	
	    private void processMiscEvents(Event evt, Session session)
	    {
	    	System.out.println("Processing " + evt.eventType().toString());
	
			MessageIterator msgIter = evt.messageIterator();

			while (msgIter.hasNext()) {
	            
				Message msg = msgIter.next();

				System.out.println("MESSAGE: " + msg);
	        }
	    }
	
	    private void sendAuthRequest(Session session, String emrsUser, String ipAddress)
	    {
	        Service authService = session.getService(d_auth);
	        Request authReq = authService.createAuthorizationRequest();
	
	        authReq.set("emrsId", emrsUser);
	        authReq.set("ipAddress", ipAddress);
	
	        this.identity = session.createIdentity();
	
	        try
	        {
	        	System.out.println("Sending Auth Request:" + authReq.toString());
	            session.sendAuthorizationRequest(authReq, this.identity, new CorrelationID(identity));
	            System.out.println("Sent Auth request.");
	        }
	        catch (Exception e)
	        {
	        	System.out.println("Unable to send authorization request: " + e.getMessage());
	        }
	    }
	
	    private void sendCreateIOI(Session session, String emsxSvc)
	    { 
	        Service service = session.getService(emsxSvc);
	        Request request = service.createRequest("updateIoi");
	
            Element handle = request.getElement("handle");
            handle.setElement("value", "c7290c03-b09f-4e92-86eb-002d16351188");

	        Element ioi = request.getElement("ioi");
	
	        // Set the good-until time of this option to 15 minutes from now
	        ioi.setElement("goodUntil", Instant.now().plusSeconds(900).toString());
	
	        // Create the option
	        Element option = ioi.getElement("instrument").setChoice("option");
	
	        option.setElement("structure", "CallSpread");
	
	        // This option has two legs. Create the first leg
	        Element leg1 = option.getElement("legs").appendElement();
	        leg1.setElement("type", "Call");
	        leg1.setElement("strike", 230);
	        leg1.setElement("expiry", "2017-11-01T00:00:00.000+00:00");
	        leg1.setElement("style", "European");
	        leg1.setElement("ratio", +1.00);
	        leg1.setElement("exchange", "LN");
	        leg1.getElement("underlying").setChoice("ticker");
	        leg1.getElement("underlying").setElement("ticker", "VOD LN Equity");
	
	        // Create the second leg
	        Element leg2 = option.getElement("legs").appendElement();
	        leg1.setElement("type", "Call");
	        leg2.setElement("strike", 240);
	        leg2.setElement("expiry", "2017-11-01T00:00:00.000+00:00");
	        leg2.setElement("style", "European");
	        leg2.setElement("ratio", -1.25);
	        leg2.setElement("exchange", "LN");
	        leg2.getElement("underlying").setChoice("ticker");
	        leg2.getElement("underlying").setElement("ticker", "VOD LN Equity");
	
	        // Create a quote consisting of a bid and an offer
	        Element bid = ioi.getElement("bid");
	        bid.getElement("price").setChoice("fixed");
	        bid.getElement("price").getElement("fixed").getElement("price").setValue(83.643);
	        bid.getElement("size").setChoice("quantity");
	        bid.getElement("size").getElement("quantity").setValue(2000);
	        bid.getElement("referencePrice").setElement("price", 202.155);
	        bid.getElement("referencePrice").setElement("currency", "GBP");
	        bid.setElement("notes", "offer notes");
	
	        // Set the offer
	        Element offer = ioi.getElement("offer");
	        offer.getElement("price").setChoice("fixed");
	        offer.getElement("price").getElement("fixed").getElement("price").setValue(83.64);
	        offer.getElement("size").setChoice("quantity");
	        offer.getElement("size").getElement("quantity").setValue(2000);
	        offer.getElement("referencePrice").setElement("price", 202.15);
	        offer.getElement("referencePrice").setElement("currency", "GBP");
	        offer.setElement("notes", "offer notes");
	
	        // Set targets
	        Element includes = ioi.getElement("targets").getElement("includes");
	
	        Element t1 = includes.appendElement();
	        t1.setChoice("acronym");
	        t1.setElement("acronym", "BLPA");
	
	        Element t2 = includes.appendElement();
	        t2.setChoice("acronym");
	        t2.setElement("acronym", "BLPB");
	
	        System.out.println("Sending Request: " + request.toString());
	
	        requestID = new CorrelationID();
	
	        // Submit the request
	        try
	        {
	            session.sendRequest(request, identity, requestID);
	            System.out.println("Request Sent.");
	        }
	        catch (Exception ex)
	        {
	        	System.err.println("Failed to send the request: " + ex.getMessage());
	        }
	    }
	}	
}
