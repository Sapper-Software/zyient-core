package io.zyient.base.core;

import io.zyient.base.core.model.Actor;
import io.zyient.base.core.model.EUserOrRole;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public abstract class ServiceBase {

    protected Actor getCurrentActor() throws SecurityException {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (!(authentication instanceof AnonymousAuthenticationToken)) {
            String username = authentication.getName();
            return new Actor(username, EUserOrRole.User);
        }
        throw new SecurityException("Anonymous user detected...");
    }
}
