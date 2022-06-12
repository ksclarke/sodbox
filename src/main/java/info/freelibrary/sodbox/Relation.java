
package info.freelibrary.sodbox;

/**
 * Class representing relation between owner and members.
 */
public abstract class Relation<M, O> extends Persistent implements Link<M> {

    private O myOwner;

    protected Relation() {
    }

    /**
     * Relation constructor. Creates empty relation with specified owner and no members. Members can be added to the
     * relation later.
     *
     * @param aOwner owner of the relation
     */
    public Relation(final O aOwner) {
        myOwner = aOwner;
    }

    /**
     * Get relation owner.
     *
     * @return owner of the relation
     */
    public O getOwner() {
        return myOwner;
    }

    /**
     * Set relation owner.
     *
     * @param aOwner new owner of the relation
     */
    public void setOwner(final O aOwner) {
        myOwner = aOwner;
        modify();
    }

}
