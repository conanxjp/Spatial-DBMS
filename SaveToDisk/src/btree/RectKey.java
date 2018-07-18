package btree;

import global.Rect;

public class RectKey extends KeyClass {

	private Rect key;


	public RectKey(Rect rectKey)
	{
		setKey(rectKey);
	}


	public String toString()
	{
		return key.toString();
	}


	//getter
	public Rect getKey()
	{
		return key;
	}

	//setter
	public void setKey(Rect rectKey)
	{
		key = rectKey;
	}


}
