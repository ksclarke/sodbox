package info.freelibrary.sodbox;

/**
 * Class representing relation between owner and members
 */
public abstract class Relation<M, O> extends Persistent implements Link<M> {

	private O owner;
	
	protected Relation() {
	}
	
	/**
	 * Relation constructor. Creates empty relation with specified owner and no
	 * members. Members can be added to the relation later.
	 * 
	 * @param owner owner of the relation
	 */
	public Relation(O owner) {
		this.owner = owner;
	}
	
	/**
	 * Get relation owner
	 * 
	 * @return owner of the relation
	 */
	public O getOwner() {
		return owner;
	}

	/**
	 * Set relation owner
	 * 
	 * @param owner new owner of the relation
	 */
	public void setOwner(O owner) {
		this.owner = owner;
		modify();
	}
}
