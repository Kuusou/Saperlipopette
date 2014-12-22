package linda.shm;

import java.util.Observable;
import java.util.Observer;

import linda.Callback;
import linda.Linda;
import linda.Linda.eventMode;
import linda.Tuple;

public class MyObserver implements Observer {

	private Tuple template;
	private Callback callback;
	private eventMode mode;
	
	public MyObserver(eventMode mode, Tuple template, Callback callback) {
		super();
		this.mode = mode;
		this.template = template;
		this.callback = callback;
	}

	@Override
	public void update(Observable o, Object arg) {
		// TODO Auto-generated method stub
		// System.out.println("object updated");
		if (o instanceof Linda) {
			Tuple tuple = (Tuple) arg;
			// si le nouveau tuple correspond au template
			if (tuple.matches(template)) {
				// System.out.println("tuple matches");
				if (mode==eventMode.READ) {
					tuple=((Linda) o).tryRead(template);
				} else {
					tuple=((Linda) o).tryTake(template);
				} 
				//on regarde si on a trouvé le tuple correspondant dans la liste (car d'autres observateurs en mode take ou des takeMatchEnAttente pourront 
				//avoir été prioritaires et l'avoir enlevé de la liste)
				// on effectue l'action puis on supprime l'observer
				if (tuple != null) {
					callback.call(tuple);
					o.deleteObserver(this);
				}
			}
		}
	}
}
