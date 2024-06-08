import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceBackendHandler implements BcryptService.Iface {
    static Logger log;

    static {
        BasicConfigurator.configure();
        log = Logger.getLogger(BcryptServiceHandler.class);
    }

    public List<String> hashPassword(List<String> password, short logRounds)
            throws IllegalArgument, org.apache.thrift.TException {
        // if no backend node is available
        // hashes all passwords in the list at FE
        List<String> hashedPasswords = new ArrayList<>();
        log.info("The password received at backend hashPassword is: " + password);
        for (String pwd : password) {
            try {
                String hashed = BCrypt.hashpw(pwd, BCrypt.gensalt(logRounds));
                log.info("hashed in hashPassword is: " + hashed);
                hashedPasswords.add(hashed);
            } catch (Exception e) {
                throw new IllegalArgument(e.getMessage());
            }
        }
        return hashedPasswords;
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash)
            throws IllegalArgument, org.apache.thrift.TException {
        // TODO: throw TException?
        if (password.size() != hash.size()) {
            throw new IllegalArgument("The number of passwords does not match the number of hashes.");
        }

        log.info("password being received at the backend in checkPaswword is: " + password);
        log.info("hash being received at the backend in checkPaswword is: " + hash);

        // TODO: Some shit failing here causing checkpassword to fail, both password and
        // hash are []
        // if no backend node is available
        // checks all passwords in the list at FE
        List<Boolean> ret = new ArrayList<>();

        for (int i = 0; i < password.size(); i++) {
            try {
                // log.info("password list is: " + password + " and hash list is: " + hash);
                String onePwd = password.get(i);
                String oneHash = hash.get(i);
                // Problem: One is hello and one is too short
                log.info("onePwd is: " + onePwd);
                log.info("oneHash is: " + oneHash);
                ret.add(BCrypt.checkpw(onePwd, oneHash));
            } catch (Exception e) {
                log.info("Error checking password for pwd: " + password.get(i) + ", hash: " + hash.get(i));
                throw new IllegalArgument(e.getMessage());
            }
        }
        return ret;
    }

    // dummy function, never used, here to fulfill a1.thrift definition.
    // Separate .thrift file?
    @Override
    public void registerBackend(String host, int port) {
        log.info("dummy function should never be called");
    }
}
