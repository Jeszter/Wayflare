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
        if (Player.instanse == null) { return; }

    switch (eventId)
    {
        case 0: // WOOD (no iron, 100 wood)
            Player.instanse.SendMapResources(0, 100, 0);
            break;

        case 1: // IRON (100 iron, no wood)
            Player.instanse.SendMapResources(100, 0, 0);
            break;

        case 2:
            Debug.Log("Player instance found, sending packet to load Castle scene.");
                Packet packet = new Packet();
                packet.Write((int)Player.RequestsID.BATTLEFIND);
                Sender.TCP_Send(packet);
            break;

        default:
            Debug.LogWarning("Unknown eventId");
            break;
    }
}

}
