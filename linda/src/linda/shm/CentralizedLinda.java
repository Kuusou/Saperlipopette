package linda.shm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import linda.Callback;
import linda.Linda;
import linda.Tuple;

/** Shared memory implementation of Linda. */
public class CentralizedLinda extends Observable implements Linda {
	
	

	Collection<Tuple> tuples;
	Map<Tuple, LinkedList<Integer>> MatchEnAttente;
	Lock moniteur;
	Condition[] classe;
	int id;
	
	public CentralizedLinda() {
		super();
		tuples = new LinkedList<Tuple>();
		MatchEnAttente = new HashMap<Tuple, LinkedList<Integer>>();
		moniteur = new ReentrantLock();
    	classe = new Condition[200];
    	id = 0;
	}

	@Override
	public void write(Tuple t) {
		// TODO Auto-generated method stub
		moniteur.lock();
		tuples.add(t);
		this.reveil(recupererTemplate(t)); // S'il n'y a personne à reveiller il ne se passe rien
		this.setChanged();
		this.notifyObservers(t);
		moniteur.unlock();
	}

	@Override
	public Tuple take(Tuple template) {
		// TODO Auto-generated method stub
		moniteur.lock();
		Tuple t = this.tryTake(template);
		int nb;
		if (t == null) {
			nb = id;
			this.id++;
			try {
				MatchEnAttente.get(template).add(nb);
			} catch (Exception e) {
				LinkedList<Integer> listes = new LinkedList<Integer>();
				listes.add(nb);
				MatchEnAttente.put(template,listes);
			}
			classe[nb] = moniteur.newCondition();			
			try {
				classe[nb].await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			//System.out.println("Take N°"+nb+" reveillé");
			t = this.tryTake(template);
			MatchEnAttente.get(template).remove(nb);
			if (MatchEnAttente.get(template).size() == 0) {
				MatchEnAttente.remove(template);
			}
		}		
		moniteur.unlock();
		return t;
	}

	@Override
	public Tuple read(Tuple template) {
		// TODO Auto-generated method stub		
		moniteur.lock();
		Tuple t = this.tryRead(template);
		int nb;
		if (t == null) {
			nb = id;
			this.id++;
			try {
				MatchEnAttente.get(template).add(nb);
			} catch (Exception e) {
				LinkedList<Integer> listes = new LinkedList<Integer>();
				listes.add(nb);
				MatchEnAttente.put(template,listes);
			}
			classe[nb] = moniteur.newCondition();			
			try {
				classe[nb].await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			//System.out.println("Read N°"+nb+" reveillé");
			t = this.tryRead(template);
			//System.out.println("Avant :"+MatchEnAttente.get(template));
			MatchEnAttente.get(template).removeFirst();
			//System.out.println("Apres :"+MatchEnAttente.get(template));
			if (MatchEnAttente.get(template).size() == 0) {
				MatchEnAttente.remove(template);
			}
			if (t!=null) {
				reveil(recupererTemplate(t));
			}
		}
		moniteur.unlock();
		return t;
	}

	@Override
	public Tuple tryTake(Tuple template) {
		// TODO Auto-generated method stub
		Tuple resultat = null;		
		Iterator<Tuple> it = tuples.iterator();
		while(it.hasNext() && resultat == null){
			Tuple t = it.next();
			if(t.matches(template)){
				resultat = t;
				tuples.remove(t);
			}
		} 		
		return resultat;
	}

	@Override
	public Tuple tryRead(Tuple template) {
		// TODO Auto-generated method stub		
		Tuple resultat = null;		
		Iterator<Tuple> it = tuples.iterator();
		while(it.hasNext() && resultat == null){
			Tuple t = it.next();
			if(t.matches(template)){
				resultat = t;
			}
		} 		
		return resultat;
	}

	@Override
	public Collection<Tuple> takeAll(Tuple template) {
		// TODO Auto-generated method stub
		Collection<Tuple> ts = new ArrayList<Tuple>();
		for (Tuple t : tuples) {
			if (t.matches(template)) {
				ts.add(t);
				tuples.remove(t);
			}
		}
		return ts;
	}

	@Override
	public Collection<Tuple> readAll(Tuple template) {
		// TODO Auto-generated method stub
		Collection<Tuple> ts = new ArrayList<Tuple>();
		for (Tuple t : tuples) {
			if (t.matches(template)) {
				ts.add(t);
			}
		}
		return ts;
	}

	@Override
	public void eventRegister(eventMode mode, eventTiming timing,
			Tuple template, Callback callback) {
		// TODO Auto-generated method stub
		Tuple tuple =null;
		Observer obs;
		
	    // dans le cas où l'action à effectuer est IMMEDIATE
		if (timing==eventTiming.IMMEDIATE) {
			if (mode==eventMode.READ) {
				tuple=this.tryRead(template);
			} else {
				tuple=this.tryTake(template);
			} 
			//si on a trouvé un tuple immediatement, on effectue l'action
			if (tuple != null) {
				callback.call(tuple);
		    //sinon on attend avec un observer
			} else { 
				obs = new MyObserver(mode, template, callback);
				this.addObserver(obs);
				// System.out.println("obs added");
			}
		// dans le cas où l'action à effectuer est FUTURE, on ajoute simplement l'observer
		} else {
			obs = new MyObserver(mode, template, callback);
			this.addObserver(obs);
		}
	}

	@Override
	public void debug(String prefix) {
		// TODO Auto-generated method stub
		
	}

	private Collection<Tuple> recupererTemplate(Tuple t) {
		Collection<Tuple> templates = new HashSet<Tuple>();
		if (t == null) {
			System.out.println("probleme recupererTemplate");
		}
		if (MatchEnAttente.keySet().isEmpty()) {
			// Rien a faire
		} else {
			for (Tuple template : MatchEnAttente.keySet()) {
				if (t.matches(template)) {
					templates.add(template);
				}
			}
		}
		return templates;
	}

    private void reveil(Collection<Tuple> templates) {
    	TreeSet<Integer> ensemble = new TreeSet<Integer>();
    	for (Tuple t : templates) {
    		ensemble.addAll(MatchEnAttente.get(t));
    	}
    	if (ensemble.isEmpty()) {
    		//Personne en attente
    	} else {
	    	//System.out.println("Reveil de : "+ensemble.first());
	    	classe[ensemble.first()].signal();
    	}
	}
    
    
	/*
    private Collection<Tuple> trouve(Tuple t) {
    	Collection<Tuple> templates = MatchEnAttente.keySet();
    	for (Tuple template : templates) {
    		if (!t.matches(template)) {
    			templates.remove(template);
    		}
    	}
    	return templates;
    }
    
    private void reveil(Tuple t) {
    	int nb = MatchEnAttente.get(t).getFirst();
    	classe[nb].signal();
    }
    */

}