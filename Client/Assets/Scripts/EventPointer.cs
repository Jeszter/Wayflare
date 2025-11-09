using UnityEngine;
using Mapbox.Utils;
using Mapbox.Examples;
using DevelopersHub.ClashOfWhatecer;

public class EventPointer : MonoBehaviour
{
    // Start is called once before the first execution of Update after the MonoBehaviour is created
    [SerializeField] float rotation = 50f;
    [SerializeField] float amplitude = 2f;
    [SerializeField] float frequency = 0.5f;
    

    [SerializeField] private ResourceID resourseID;
    LocationStatus playerLocation;
    public Vector2d eventPos;


    UI_Main ui;

    public enum ResourceID
    {
            Wood = 0, Iron = 1, Castl = 2 , Enemmy = 3
    }

    void Start()
    {
        ui = GameObject.Find("Main").GetComponent<UI_Main>();
        Debug.Log(resourseID+ " : "+ ((int) resourseID));
    }

    // Update is called once per frame
    void Update()
    {
        if (ResourceID.Castl != resourseID && ResourceID.Enemmy != resourseID) {
            FloatAndRotatedPointer();
        }
    }

    void FloatAndRotatedPointer()
    {
        transform.Rotate(Vector3.up, (rotation + Random.Range(-10f,10f))* Time.deltaTime);
        transform.position = new Vector3(transform.position.x, Mathf.Sin(Time.fixedTime * Mathf.PI * frequency) * amplitude + 5, transform.position.z);
    }

    private void OnMouseDown()
    {   

        Debug.Log(resourseID);
        playerLocation = GameObject.Find("Canvas").GetComponent<LocationStatus>();
        var currPlayerLocation = new GeoCoordinatePortable.GeoCoordinate(playerLocation.GetLocationLat(), playerLocation.GetLocationLon());
        var eventLocation = new GeoCoordinatePortable.GeoCoordinate(eventPos[0], eventPos[1]);
        var distance = currPlayerLocation.GetDistanceTo(eventLocation);
        // Debug.Log("event: " + eventPos[0] + " ; " + eventPos[1]);
        // Debug.Log("player: " + playerLocation.GetLocationLat() + " ; " + playerLocation.GetLocationLon());
        // Debug.Log("Distance " + distance);
        if (distance < 150)
        {
            ui.DisplayInRangeEvent((int) resourseID);
        }
        else
        {
            ui.DisplayNotInRange();
        }
    }
}


