package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (LockType.substitutable(effectiveLockType, requestType)) return;

        LockType newType;
        if (requestType == LockType.S){
            if (effectiveLockType == LockType.IS || effectiveLockType == LockType.NL) newType = LockType.S;
            else if(effectiveLockType == LockType.IX) newType = LockType.SIX;
            else return;
        } else {
            newType = LockType.X;
        }
        if (newType == explicitLockType) return;

        // Deal with parents
        ensureParentLock(transaction, lockContext, parentContext, newType);

        // Deal with current resource
        if (newType == LockType.S){
            if(effectiveLockType == LockType.IS){
                lockContext.escalate(transaction);
            } else {
                lockContext.acquire(transaction, LockType.S);
            }
        } else if (newType == LockType.SIX){
            lockContext.promote(transaction, LockType.SIX);
        } else {
            //newType = X
            if (effectiveLockType == LockType.NL){
                lockContext.acquire(transaction, LockType.X);
            } else {
                lockContext.escalate(transaction);
                if (lockContext.getEffectiveLockType(transaction) == LockType.S) {
                    lockContext.promote(transaction, LockType.X);
                }
            }
        }
    }

    public static void ensureParentLock(TransactionContext transaction, LockContext lockContext, LockContext parentContext, LockType requestType){
        if (requestType == LockType.SIX) return;

        // S request
        if (requestType == LockType.S){
            List<LockContext> needIsContext = new ArrayList<>();
            while (parentContext != null){
                LockType type = parentContext.getExplicitLockType(transaction);
                if (type == LockType.NL) needIsContext.add(0, parentContext);
                parentContext = parentContext.parentContext();
            }
            for(LockContext ctx: needIsContext){
                ctx.acquire(transaction, LockType.IS);
            }
        }

        // X request
        if (requestType == LockType.X){
            List<Pair<LockContext, String>> needModifyContext = new ArrayList<>();
            // "A": acquire IX "B": promote SIX "C": promote IX
            while (parentContext != null){
                LockType type = parentContext.getExplicitLockType(transaction);
                if (type == LockType.NL) {
                    needModifyContext.add(0, new Pair<>(parentContext, "A" ));
                } else if (type == LockType.S) {
                    needModifyContext.add(0, new Pair<>(parentContext, "B" ));
                } else if (type == LockType.IS) {
                    needModifyContext.add(0, new Pair<>(parentContext, "C" ));
                }
                parentContext = parentContext.parentContext();
            }

            for(Pair<LockContext, String> item: needModifyContext){
                LockContext ctx = item.getFirst();
                String flag = item.getSecond();
                switch(flag){
                    case "A": ctx.acquire(transaction, LockType.IX);break;
                    case "B": ctx.promote(transaction, LockType.SIX);break;
                    case "C": ctx.promote(transaction, LockType.IX);break;
                }
            }
        }
    }

}
