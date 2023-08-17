package ai.sapper.cdc.intake.templates;

import ai.sapper.cdc.intake.model.EmailJson;
import ai.sapper.cdc.intake.utils.DateUtils;
import com.google.common.base.Strings;


import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class MailTemplateHelper {
    public static final String VAR_PREFIX = "m";
    public static final String VAR_SENDER_NAME = "sender_name";
    public static final String VAR_SENDER_EMAIL = "sender_email";
    public static final String VAR_DATE = "date";
    public static final String VAR_SUBJECT = "subject";
    public static final String VAR_SIGN = "signature";

    public static Map<String, String> generateContext(@Nonnull EmailJson email,
                                                      String signature,
                                                      String dateFormat) {
        Map<String, String> map = new HashMap<>();
        map.put(getVar(VAR_SENDER_NAME), email.getSenderName());
        map.put(getVar(VAR_SENDER_EMAIL), email.getSenderEmail().toString());
        if (!Strings.isNullOrEmpty(dateFormat))
            map.put(getVar(VAR_DATE), DateUtils.formatTimestamp(dateFormat));
        else
            map.put(getVar(VAR_DATE), DateUtils.formatTimestamp());
        map.put(getVar(VAR_SUBJECT), email.getHeader().getSubject());
        if (!Strings.isNullOrEmpty(signature)) {
            map.put(getVar(VAR_SIGN), signature);
        }
        return map;
    }

    private static String getVar(String name) {
        return String.format("%s_%s", VAR_PREFIX, name);
    }
}
