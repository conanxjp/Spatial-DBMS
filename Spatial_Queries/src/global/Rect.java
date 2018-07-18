package global;


public class Rect
{
    public double crdx1;
    public double crdy1;
    public double crdx2;
    public double crdy2;
    
    public Rect(){
    	
    }
    //x1 must <= x2 and y1 must <= y2
    public Rect (double x1, double y1, double x2, double y2)
    {
        crdx1 = x1;
        crdy1 = y1;
        crdx2 = x2;
        crdy2 = y2;
        if(x1 > x2 || y1 > y2){
        	System.out.printf("Error Create rectangle! the first points must be less than the second point.\n");
        	System.exit(1);
        	}
        	
    }

    public double area ()
    {
        return Math.abs((crdx2 - crdx1) * (crdy2 - crdy1));
    }
    
    public Rect intersect(Rect another){
    	Rect result = null;
    	//must this.x1 <= other.x1, otherwise call other.intersect(this);
    	//simplify number of cases be 2;
    	if (crdx1 <= another.crdx1)
    	{	  
    		if(crdy1 <= another.crdy1)
    		{
    			if(crdx2 <= another.crdx2)
    			{    				
    				if(crdy2 <= another.crdy2)
    				{
    					//case 1: this left bottem, another right up
    					if( crdx2 <= another.crdx1 || crdy2 <= another.crdy1)
    						result = null;
    					else
    						result = new Rect(another.crdx1, another.crdy1, crdx2,crdy2);
    					
    				}
    				else { // crdy2 > another.crdy2
    					if(crdx2 > another.crdx1)
    						//case 2: this left, another right middle
    						if(crdx2 < another.crdx1)
    							result = null;
    						else
    							result = new Rect(another.crdx1, another.crdy1, crdx2, another.crdy2);
    					
    				}    				    					
    			}
    			
    			else{ // crdx2 > another.crdx2
    				if(crdy2 <= another.crdy2){
    					//case 3: this bottom, another up middle
    					if(crdy2 <= another.crdy1 )
    						result = null;
    					else
    						result = new Rect(another.crdx1, another.crdy1, another.crdx2, crdy2);
    					
    				}
    				else //case 4: this contains another
    					result = new Rect(another.crdx1, another.crdy1, another.crdx2, another.crdy2);
    			}
    			
    		}
    		else  //crdy1 > another.crdy1
    		{	
    			if(crdx2 <= another.crdx2)
    			{	if (crdy2 <= another.crdy2)
    				{	
    					//case 5: this rect lefe middle, another rect right
    					if(crdx2 <= another.crdx1)
    					{
    					result = null;
    					}
    				
    					else
    						result = new Rect(another.crdx1, crdy1, crdx2,crdy2);
    				}
    			
    				else //crdy2 > another.crdy2
    				{	
    					//case 6:this rect left up, another rect bottem right
    					if(crdx2 <= another.crdx1 || another.crdy2 <= crdy1)
    						result = null;
    					else
    						result = new Rect(another.crdx1, crdy1, crdx2, another.crdy2);
    				}
    			}
    			else // crdx2 > another.crdx2
    			{
    				if( crdy2 <= another.crdy2)
    				{
    					//case 7: this rect horizontal middle, another vertical middle
    					result = new Rect(another.crdx1, crdy1, another.crdx2, crdy2);
    				}
    				else	//crdy2 > another.crdy2
    				{
    					//case 8 this rect up, another bottom middle
    					if (another.crdy2 <= crdy1)
    						result = null;    						
    					else
    						result = new Rect(another.crdx1, crdy1, another.crdx2, another.crdy2);
    				}
    					
    			}
    		
    			
    		}
    		
    	}
    	else 
    	{
    		return another.intersect(this);
    	}
    
    	return result;
    }
    
    public double distance(Rect another){
    	double result;
    	if(crdx1 <= another.crdx1){	//this rect must be in the left
    		
    		Rect tmp = this.intersect(another);
        	if(tmp == null)
        		result = 0;
        	else{
        		//case 1 another rect lies in right of this rect
        		//		 distance is the x-axle distance
        		if (crdx2 <=another.crdx1 && another.crdy1 <= crdy1 )
        			result = another.crdx1 - crdx2;
        		
        		//case 2: another rect lies in upper of this rect
        		//		  distance is the y-axle distance
        		else if(crdy2 <= another.crdy1 && another.crdx1 <= crdx2)
        			result = another.crdy1 - crdy2;
        		
        		//case 3: distance between two points
        		else
        			result = Math.sqrt((another.crdx1-crdx2)*(another.crdx1-crdx2) +
        					(another.crdy1 - crdy2)*(another.crdy1 - crdy2));
        	}
        	
        	
    	}
    	else
    		return another.distance(this);
    		
    	return result;
    }
    
    public String toString()
    {
	    return Double.toString(crdx1) + "," + Double.toString(crdy1) + "," + Double.toString(crdx2) + "," + Double.toString(crdy2);
    }
    
}
