//--
//      Copyright (C) 2016 DataStax Inc.
//
//      This software can be used solely with DataStax Enterprise. Please consult the license at
//      http://www.datastax.com/terms/datastax-dse-driver-license-terms
//++

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.Subject;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.jruby.Ruby;
import org.jruby.RubyNil;
import org.jruby.RubyString;
import org.jruby.RubyFixnum;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.javasupport.JavaObject;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

public class ChallengeEvaluatorService implements BasicLibraryService
{
    public boolean basicLoad(final Ruby runtime) throws IOException
    {
        RubyModule currentContainer;
        currentContainer = runtime.defineModule("Dse");
        currentContainer = runtime.defineModuleUnder("Auth", currentContainer);
        currentContainer = runtime.defineModuleUnder("Providers", currentContainer);
        RubyModule challengeEvaluator = runtime.defineModuleUnder("ChallengeEvaluator", currentContainer);

        challengeEvaluator.defineAnnotatedMethods(ChallengeEvaluator.class);

        return true;
    }

    @JRubyModule(name="ChallengeEvaluator")
    public static class ChallengeEvaluator
    {
        @JRubyMethod(name="evaluate", module=true)
        public static RubyString evaluate(ThreadContext context, IRubyObject self, IRubyObject saslClientRuby, IRubyObject subjectRuby, IRubyObject challengeRuby)
        {
            final SaslClient saslClient = (SaslClient) saslClientRuby.toJava(SaslClient.class);
            final Subject subject = (Subject) subjectRuby.toJava(Subject.class);
            final byte[] challenge = challengeRuby.convertToString().getBytes();

            try {
                byte[] result = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                    public byte[] run() throws SaslException {
                        return saslClient.evaluateChallenge(challenge);
                    }
                });
                return RubyString.newString(context.runtime, result);
            } catch (PrivilegedActionException e) {
                throw new RuntimeException(e.getException());
            }
        }

        @JRubyMethod(name="make_configuration", module=true)
        public static IRubyObject makeConfiguration(ThreadContext context, IRubyObject self, IRubyObject principalRuby, IRubyObject ticketCacheRuby)
        {
            final String principal = (principalRuby instanceof RubyNil) ? null : principalRuby.convertToString().toString();
            final String ticketCache = (ticketCacheRuby instanceof RubyNil) ? null : ticketCacheRuby.convertToString().toString();
            Configuration config = new Configuration() {
                @Override
                public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                    Map<String, String> options = new HashMap<String, String>();
                    if (principal != null && !principal.isEmpty()) {
                        options.put("principal", principal);
                    }
                    if (ticketCache != null && !ticketCache.isEmpty()) {
                        options.put("ticketCache", ticketCache);
                    }
                    options.put("useTicketCache", "true");
                    options.put("renewTGT", "true");
                    options.put("doNotPrompt", "true");

                    return new AppConfigurationEntry[]{
                            new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)
                            };
                }
            };
            return JavaObject.wrap(context.runtime, config);
        }
    }
}
