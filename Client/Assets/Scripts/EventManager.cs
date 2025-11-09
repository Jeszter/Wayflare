using UnityEngine;
using UnityEngine.SceneManagement;
using DevelopersHub.ClashOfWhatecer;
using DevelopersHub.RealtimeNetworking.Client;   // <-- �����, ����� ������ ����� Player

public class EventManager : MonoBehaviour
{
    void Start()
    {

    }

    void Update()
    {

    }

public void ActivateEvent(int eventId)
{
    Debug.Log($"ActivateEvent: {eventId}");

    switch (eventId)
    {
        case 0: // WOOD (no iron, 100 wood)
            if (Player.instanse != null)
            {
                Player.instanse.SendMapResources(0, 100, 0);
                Debug.Log("Resources received from the map: 0 iron, 100 wood, 0 gold");
            }
            else
            {
                Debug.LogWarning("Player instance is null — failed to send resources!");
            }
            break;

        case 1: // IRON (100 iron, no wood)
            if (Player.instanse != null)
            {
                Player.instanse.SendMapResources(100, 0, 0);
                Debug.Log("Resources received from the map: 100 iron, 0 wood, 0 gold");
            }
            else
            {
                Debug.LogWarning("Player instance is null — failed to send resources!");
            }
            break;

        case 2:
            Debug.Log("Loading Castle Scene...");
            if (Player.instanse != null)
            {
                Debug.Log("Player instance found, sending packet to load Castle scene.");
                Packet packet = new Packet();
                packet.Write((int)Player.RequestsID.BATTLEFIND);
                Sender.TCP_Send(packet);
            }
            else
            {
                Debug.LogWarning("Player instance is null — failed to send load request!");
            }
            break;

        default:
            Debug.LogWarning("Unknown eventId");
            break;
    }
}

}
